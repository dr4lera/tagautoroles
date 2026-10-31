import os
import discord
from discord import app_commands
from discord.ext import commands
import aiohttp
import logging
import sqlite3
import asyncio
import json
import time
import traceback


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()
logging.getLogger('discord').setLevel(logging.DEBUG)  


intents = discord.Intents.default()
intents.members = True
bot = commands.AutoShardedBot(command_prefix='!', intents=intents)  


TOKEN = os.environ.get('DISCORD_TOKEN', '')
if not TOKEN:
    raise SystemExit('Set the DISCORD_TOKEN environment variable')  


conn = sqlite3.connect('server_configs.db', check_same_thread=False)  
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS server_config (
        guild_id TEXT PRIMARY KEY,
        role_name TEXT,
        badge TEXT,
        tag TEXT
    )
''')
conn.commit()


CHECK_LIMIT = 10000 
TIME_WINDOW = 600  
MIN_DELAY_PER_REQUEST = 1 / 50  


async def fetch_user_data(user_id):
    url = f"https://discord.com/api/v9/users/{user_id}"
    headers = {"Authorization": f"Bot {TOKEN}"}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        logger.info(f"Data fetched for User ID: {user_id} -> {data}")
                        return data
                    elif response.status == 429:
                        retry_after = float(response.headers.get('Retry-After', 1))
                        logger.warning(f"Rate limit exceeded (429). Retrying after {retry_after} seconds...")
                        await asyncio.sleep(retry_after)
                        continue
                    else:
                        logger.error(f"Failed to fetch data for {user_id}: {response.status}")
                        raise Exception(f"Failed to fetch data. Status Code: {response.status}")
            except aiohttp.ClientError as e:
                logger.error(f"Network error fetching data for {user_id}: {e}")
                raise


async def check_member(member, role, role_name, badge, tag):
    logger.info(f"Checking user: {member.display_name} | ID: {member.id}")
    user_data = await fetch_user_data(member.id)

    if user_data and user_data.get('clan'):
        logger.info(f"User Data for {member.display_name}: {user_data}")
        clan_data = user_data.get('clan', {})
        user_badge = str(clan_data.get('badge', '')).strip()
        user_tag = str(clan_data.get('tag', '')).strip()
        stored_badge = str(badge).strip()
        stored_tag = str(tag).strip()

        logger.info(f"Comparing - User Badge: {user_badge} | Stored Badge: {stored_badge} | User Tag: {user_tag} | Stored Tag: {stored_tag}")

        if user_badge == stored_badge and user_tag == stored_tag:
            if role not in member.roles:
                logger.info(f"Badge and tag match for {member.display_name}. Adding role '{role_name}'...")
                await member.add_roles(role)
                logger.info(f"Successfully added role '{role_name}' to {member.display_name}")
            else:
                logger.info(f"User {member.display_name} already has role '{role_name}'")
        else:
            if role in member.roles:
                logger.info(f"Badge or tag does not match for {member.display_name}. Removing role '{role_name}'...")
                await member.remove_roles(role)
                logger.info(f"Successfully removed role '{role_name}' from {member.display_name}")
            else:
                logger.info(f"User {member.display_name} does not match badge/tag, and does not have role '{role_name}'")
    else:
        logger.warning(f"No user data fetched for {member.display_name} (ID: {member.id}). Skipping...")


def get_guild_config(guild):
    cursor.execute("SELECT role_name, badge, tag FROM server_config WHERE guild_id = ?", (str(guild.id),))
    config = cursor.fetchone()
    if not config:
        return None
    role_name, badge, tag = config
    role = discord.utils.get(guild.roles, name=role_name)
    if not role:
        return None
    return role, role_name, badge, tag


async def check_single_member(member):
    config = get_guild_config(member.guild)
    if not config:
        return
    try:
        await check_member(member, *config)
    except Exception as e:
        logger.error(f"Error while processing user {member.display_name} (ID: {member.id}): {e}")


async def scan_all_users(guild):
    cursor.execute("SELECT role_name, badge, tag FROM server_config WHERE guild_id = ?", (str(guild.id),))
    config = cursor.fetchone()
    if not config:
        logger.info(f"No configuration found for guild {guild.name} (ID: {guild.id}).")
        return

    role_name, badge, tag = config
    role = discord.utils.get(guild.roles, name=role_name)
    if not role:
        logger.warning(f"Role '{role_name}' not found in guild {guild.name}. Skipping...")
        return

    total_members = len(guild.members)
    if total_members > CHECK_LIMIT:
        logger.warning(f"Total members ({total_members}) exceed check limit ({CHECK_LIMIT}). Only checking first {CHECK_LIMIT} members.")
        total_members = CHECK_LIMIT

    min_delay_per_check = max(MIN_DELAY_PER_REQUEST, 0.1)
    total_time = TIME_WINDOW
    delay_per_check = max(min_delay_per_check, total_time / total_members)
    logger.info(f"Scanning {total_members} members in guild {guild.name} with delay of {delay_per_check:.2f} seconds per check.")

    members_checked = 0
    for member in guild.members:
        if members_checked >= CHECK_LIMIT:
            logger.info(f"Reached check limit of {CHECK_LIMIT} for guild {guild.name}. Stopping checks.")
            break

        try:
            await check_member(member, role, role_name, badge, tag)
            members_checked += 1
            await asyncio.sleep(delay_per_check)
        except Exception as e:
            logger.error(f"Error while processing user {member.display_name} (ID: {member.id}): {e}")
            continue

    logger.info(f"Completed scan of {members_checked} members in guild {guild.name}.")


RECHECK_COOLDOWN = 60
last_checked = {}


@bot.event
async def on_member_join(member):
    logger.info(f"New member joined: {member.display_name} (ID: {member.id}) in guild {member.guild.name}")
    await check_single_member(member)

@bot.event
async def on_member_update(before, after):
    # only re-check the member that changed, at most once per RECHECK_COOLDOWN seconds
    key = (after.guild.id, after.id)
    now = time.monotonic()
    if now - last_checked.get(key, 0) < RECHECK_COOLDOWN:
        return
    last_checked[key] = now
    await check_single_member(after)

def cleanup_invalid_badges():
    cursor.execute("SELECT guild_id, badge FROM server_config WHERE LENGTH(badge) < 3")
    invalid_entries = cursor.fetchall()
    for guild_id, badge in invalid_entries:
        logger.warning(f"Removing invalid badge '{badge}' from guild ID: {guild_id}")
        cursor.execute("DELETE FROM server_config WHERE guild_id = ?", (guild_id,))
    if invalid_entries:
        conn.commit()
        logger.info(f"Cleanup complete. Removed {len(invalid_entries)} invalid badge entries.")
    else:
        logger.info("No invalid badge entries found during cleanup.")

@bot.event
async def on_ready():
    await bot.tree.sync()
    cleanup_invalid_badges()
    logger.info(f"Bot is online as {bot.user} with {bot.shard_count} shards")
    for shard_id in bot.shard_ids:
        logger.info(f"Shard {shard_id} is ready, handling {len([g for g in bot.guilds if (g.id >> 22) % bot.shard_count == shard_id])} guilds")

@bot.tree.command(name='setup', description='Configure badge, tag, and role for tracking')
@app_commands.default_permissions(manage_roles=True)
@app_commands.checks.has_permissions(manage_roles=True)
async def setup(interaction: discord.Interaction, badge: str, tag: str):
    cleanup_invalid_badges()
    if len(badge) < 3:
        await interaction.response.send_message("Badge must be at least 3 characters long.", ephemeral=True)
        logger.warning(f"Setup attempt failed: Badge '{badge}' is less than 3 characters.")
        return
    guild_id = str(interaction.guild_id)
    cursor.execute("REPLACE INTO server_config (guild_id, role_name, badge, tag) VALUES (?, (SELECT role_name FROM server_config WHERE guild_id = ?), ?, ?)", (guild_id, guild_id, badge, tag))
    conn.commit()
    await interaction.response.send_message(f"Configuration set: Badge = {badge}, Tag = {tag}")
    logger.info(f"Configuration set for Guild {guild_id}: Badge = {badge}, Tag = {tag}")

@bot.tree.command(name='tagrole', description='Set the role to apply for tagged users')
@app_commands.default_permissions(manage_roles=True)
@app_commands.checks.has_permissions(manage_roles=True)
async def tagrole(interaction: discord.Interaction, role: discord.Role):
    guild_id = str(interaction.guild_id)
    cursor.execute("REPLACE INTO server_config (guild_id, role_name, badge, tag) VALUES (?, ?, (SELECT badge FROM server_config WHERE guild_id = ?), (SELECT tag FROM server_config WHERE guild_id = ?))", (guild_id, role.name, guild_id, guild_id))
    conn.commit()
    await interaction.response.send_message(f"✅Role set to: {role.name}. All users will be checked on next event.")
    logger.info(f"✅Role set to: {role.name} for Guild {guild_id}")

@bot.tree.command(name='forcereload', description='Force a reload of all users in this guild to update roles')
@app_commands.default_permissions(manage_roles=True)
@app_commands.checks.has_permissions(manage_roles=True)
async def forcereload(interaction: discord.Interaction):
    await interaction.response.defer()  
    await scan_all_users(interaction.guild)
    await interaction.followup.send(f"Force reload complete. All users checked in guild {interaction.guild.name}.")
    logger.info(f"Force reload completed for guild {interaction.guild.name}.")

@bot.tree.command(name='id', description='Fetches user data from Discord API so you can get the string for /setup')
@app_commands.default_permissions(manage_roles=True)
@app_commands.checks.has_permissions(manage_roles=True)
async def id(interaction: discord.Interaction, user_id: str):
    try:
        user_data = await fetch_user_data(user_id)
        formatted_data = json.dumps(user_data, indent=4)
        await interaction.response.send_message(f"```json\n{formatted_data}\n```", ephemeral=True)
        logger.info(f"Data sent for user ID: {user_id}")
    except Exception as e:
        await interaction.response.send_message(f"Failed to fetch user data: {str(e)}", ephemeral=True)
        logger.error(f"Error fetching data for user ID: {user_id} - {str(e)}")


async def start_bot():
    max_retries = 5
    base_delay = 5
    for attempt in range(max_retries):
        try:
            logger.info(f"Starting bot (Attempt {attempt + 1}/{max_retries})...")
            await bot.start(TOKEN)
            return
        except discord.errors.HTTPException as e:
            if e.status == 429:
                retry_after = float(e.retry_after or base_delay)
                logger.warning(f"Rate limit exceeded (429) on login. Retrying after {retry_after} seconds...")
                await asyncio.sleep(retry_after)
                continue
            else:
                logger.error(f"HTTP error during login: {str(e)}")
                raise
        except Exception as e:
            logger.error(f"Bot crashed with error: {str(e)}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            raise
        finally:
            try:
                await bot.close()
                logger.info("Bot session closed.")
            except Exception as close_error:
                logger.error(f"Error closing bot session: {close_error}")
    logger.error(f"Failed to start bot after {max_retries} attempts.")
    raise Exception("Max retries reached for bot login.")

def main():
    RESTART_DELAY = 30
    while True:
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(start_bot())
            loop.close()
            break
        except Exception as e:
            logger.error(f"Restarting bot in {RESTART_DELAY} seconds...")
            time.sleep(RESTART_DELAY)
            continue
        finally:
            if not loop.is_closed():
                loop.close()

if __name__ == "__main__":
    main()
