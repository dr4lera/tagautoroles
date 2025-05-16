import discord
from discord.ext import tasks, commands
import requests
import logging
import sqlite3
import asyncio
import json
from collections import deque
import time
import traceback  # Added for detailed crash logging

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

# Bot configuration
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)

# Settings
TOKEN = 'ur token here'

# Database Setup
conn = sqlite3.connect('server_configs.db')
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

# Rate limiting setup
CHECK_LIMIT = 10000  # Max checks per 10 minutes
INVALID_REQUEST_LIMIT = 10000  # Max invalid requests per 10 minutes
TIME_WINDOW = 600  # 10 minutes in seconds
REQUESTS_PER_SECOND = 50  # Discord global rate limit: 50 requests per second
MIN_DELAY_PER_REQUEST = 1 / REQUESTS_PER_SECOND  # 20ms (0.02 seconds) per request

# Track timestamps for checks and invalid requests
check_timestamps = deque()  # For total checks
invalid_request_timestamps = deque()  # For invalid requests
request_timestamps = deque()  # For per-second rate limiting

# Helper function to enforce per-second rate limit (50 requests per second)
async def enforce_per_second_rate_limit():
    current_time = time.time()
    # Remove timestamps older than 1 second
    while request_timestamps and current_time - request_timestamps[0] > 1:
        request_timestamps.popleft()
    
    # If we're at the limit, wait until enough time has passed
    while len(request_timestamps) >= REQUESTS_PER_SECOND:
        oldest_time = request_timestamps[0]
        wait_time = 1 - (current_time - oldest_time)
        if wait_time > 0:
            await asyncio.sleep(wait_time)
        current_time = time.time()
        while request_timestamps and current_time - request_timestamps[0] > 1:
            request_timestamps.popleft()
    
    # Add the current timestamp
    request_timestamps.append(current_time)

# Helper function to enforce total check rate limit (10,000 per 10 minutes)
async def enforce_check_rate_limit():
    current_time = time.time()
    # Remove timestamps older than 10 minutes
    while check_timestamps and current_time - check_timestamps[0] > TIME_WINDOW:
        check_timestamps.popleft()
    
    # If we're at the limit, wait until enough time has passed
    while len(check_timestamps) >= CHECK_LIMIT:
        oldest_time = check_timestamps[0]
        wait_time = TIME_WINDOW - (current_time - oldest_time)
        if wait_time > 0:
            logger.info(f"Check rate limit reached. Waiting for {wait_time:.2f} seconds...")
            await asyncio.sleep(wait_time)
        current_time = time.time()
        while check_timestamps and current_time - check_timestamps[0] > TIME_WINDOW:
            check_timestamps.popleft()
    
    # Add the current timestamp
    check_timestamps.append(current_time)

# Helper function to enforce invalid request rate limit (10,000 per 10 minutes)
async def enforce_invalid_request_limit():
    current_time = time.time()
    # Remove timestamps older than 10 minutes
    while invalid_request_timestamps and current_time - invalid_request_timestamps[0] > TIME_WINDOW:
        invalid_request_timestamps.popleft()
    
    # If we're at the limit, wait until enough time has passed
    while len(invalid_request_timestamps) >= INVALID_REQUEST_LIMIT:
        oldest_time = invalid_request_timestamps[0]
        wait_time = TIME_WINDOW - (current_time - oldest_time)
        if wait_time > 0:
            logger.warning(f"Invalid request rate limit reached. Waiting for {wait_time:.2f} seconds...")
            await asyncio.sleep(wait_time)
        current_time = time.time()
        while invalid_request_timestamps and current_time - invalid_request_timestamps[0] > TIME_WINDOW:
            invalid_request_timestamps.popleft()

# API Request with 429 handling
def fetch_user_data(user_id):
    url = f"https://discord.com/api/v9/users/{user_id}"
    headers = {
        "Authorization": f"Bot {TOKEN}"
    }
    
    while True:
        response = requests.get(url, headers=headers)
        
        if response.status_code == 200:
            logger.info(f"Data fetched for User ID: {user_id} -> {response.json()}")
            return response.json()
        elif response.status_code == 429:  # Rate limit exceeded
            retry_after = response.headers.get('Retry-After', 1)  # Default to 1 second if header is missing
            logger.warning(f"Rate limit exceeded (429). Retrying after {retry_after} seconds...")
            time.sleep(float(retry_after))
            continue
        else:
            logger.error(f"Failed to fetch data for {user_id}: {response.status_code}")
            invalid_request_timestamps.append(time.time())  # Track invalid request
            raise Exception(f"Failed to fetch data. Status Code: {response.status_code}")

# Background task to check users
@tasks.loop(hours=1)  # Run every hour
async def check_users():
    cursor.execute("SELECT guild_id, role_name, badge, tag FROM server_config")
    configs = cursor.fetchall()
    if not configs:
        logger.info("No guild configurations found in server_config.")
        return

    # Calculate total members to check
    total_members = 0
    for guild_id, _, _, _ in configs:
        guild = bot.get_guild(int(guild_id))
        if guild:
            total_members += len(guild.members)

    if total_members == 0:
        logger.info("No members to check across configured guilds.")
        return

    # Cap at the check limit per 10-minute window
    if total_members > CHECK_LIMIT:
        logger.warning(f"Total members ({total_members}) exceed check limit ({CHECK_LIMIT}) per 10-minute window. Only checking first {CHECK_LIMIT} members in this cycle.")
        total_members = CHECK_LIMIT

    # Calculate delay to spread checks, respecting rate limits
    min_delay_per_check = max(MIN_DELAY_PER_REQUEST, 0.1)  # At least 100ms, but respect 20ms minimum
    total_time = TIME_WINDOW  # 600 seconds (10 minutes), as checks are capped per 10-minute window
    delay_per_check = max(min_delay_per_check, total_time / total_members)
    logger.info(f"Starting hourly refresh for {total_members} members with a delay of {delay_per_check:.2f} seconds per check.")

    members_checked = 0
    for guild_id, role_name, badge, tag in configs:
        guild = bot.get_guild(int(guild_id))
        if not guild:
            logger.warning(f"Guild with ID {guild_id} not found. Skipping...")
            continue

        role = discord.utils.get(guild.roles, name=role_name)
        if not role:
            logger.warning(f"Role '{role_name}' not found in guild {guild.name}. Skipping...")
            continue

        logger.info(f"Refreshing guild: {guild.name} | Role: {role_name} | Badge: {badge} | Tag: {tag}")

        for member in guild.members:
            if members_checked >= CHECK_LIMIT:
                logger.info(f"Reached check limit of {CHECK_LIMIT} for this 10-minute window. Stopping checks for this cycle.")
                break

            try:
                # Enforce rate limits
                await enforce_check_rate_limit()  # 10,000 checks per 10 minutes
                await enforce_invalid_request_limit()  # 10,000 invalid requests per 10 minutes
                await enforce_per_second_rate_limit()  # 50 requests per second
                
                logger.info(f"Checking user: {member.display_name} | ID: {member.id}")

                # Run the synchronous fetch_user_data in an executor
                loop = asyncio.get_running_loop()
                user_data = await loop.run_in_executor(None, fetch_user_data, member.id)
                
                if user_data:
                    logger.info(f"User Data for {member.display_name}: {user_data}")
                    clan_data = user_data.get('clan', {}) or user_data.get('primary_guild', {})
                    user_badge = str(clan_data.get('badge', '')).strip()
                    user_tag = str(clan_data.get('tag', '')).strip()
                    stored_badge = str(badge).strip()
                    stored_tag = str(tag).strip()

                    logger.info(f"Comparing - User Badge: {user_badge} | Stored Badge: {stored_badge} | User Tag: {user_tag} | Stored Tag: {stored_tag}")

                    if user_badge == stored_badge and user_tag == stored_tag:
                        if role not in member.roles:
                            logger.info(f"Badge and tag match for {member.display_name}. Adding role '{role_name}' now...")
                            await member.add_roles(role)
                            logger.info(f"Successfully added role '{role_name}' to {member.display_name}")
                        else:
                            logger.info(f"User {member.display_name} already has role '{role_name}'")
                    else:
                        if role in member.roles:
                            logger.info(f"Badge or tag does not match for {member.display_name}. Removing role '{role_name}' now...")
                            await member.remove_roles(role)
                            logger.info(f"Successfully removed role '{role_name}' from {member.display_name}")
                        else:
                            logger.info(f"User {member.display_name} does not match badge/tag, and does not have role '{role_name}'")
                else:
                    logger.warning(f"No user data fetched for {member.display_name} (ID: {member.id}). Skipping...")
                
                members_checked += 1
                await asyncio.sleep(delay_per_check)  # Delay to spread checks evenly
            except Exception as e:
                logger.error(f"Error while processing user {member.display_name} (ID: {member.id}): {e}")
                continue

    logger.info(f"Completed hourly refresh, checked {members_checked} members in this cycle.")

# Instant detection of changes
@bot.event
async def on_member_update(before, after):
    if before.display_name != after.display_name or before.roles != after.roles:
        logger.info(f"Detected change in: {after.display_name}")
        loop = asyncio.get_running_loop()
        try:
            # Enforce rate limits for this API call
            await enforce_per_second_rate_limit()  # 50 requests per second
            await enforce_invalid_request_limit()  # 10,000 invalid requests per 10 minutes
            user_data = await loop.run_in_executor(None, fetch_user_data, after.id)

            cursor.execute("SELECT role_name, badge, tag FROM server_config WHERE guild_id = ?", (str(after.guild.id),))
            config = cursor.fetchone()

            if config:
                role_name, badge, tag = config
                role = discord.utils.get(after.guild.roles, name=role_name)

                if user_data:
                    clan_data = user_data.get('clan', {}) or user_data.get('primary_guild', {})
                    user_badge = str(clan_data.get('badge', '')).strip()
                    user_tag = str(clan_data.get('tag', '')).strip()
                    stored_badge = str(badge).strip()
                    stored_tag = str(tag).strip()

                    if user_badge == stored_badge and user_tag == stored_tag:
                        if role not in after.roles:
                            await after.add_roles(role)
                            logger.info(f"Instantly added role '{role_name}' to {after.display_name}")
                    elif role in after.roles:
                        await after.remove_roles(role)
                        logger.info(f"Instantly removed role '{role_name}' from {after.display_name}")
        except Exception as e:
            logger.error(f"Error in on_member_update for {after.display_name} (ID: {after.id}): {e}")

# Bot events
@bot.event
async def on_ready():
    await bot.tree.sync()
    check_users.start()
    logger.info(f"Bot is online as {bot.user}")

# Register application command to setup badge, tag, and role
def cleanup_invalid_badges():
    """
    Removes any configurations with badge strings less than 3 characters from the database.
    """
    cursor.execute("SELECT guild_id, badge FROM server_config WHERE LENGTH(badge) < 31")
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
    check_users.start()
    cleanup_invalid_badges()  # 🔄 Clean up invalid badges on startup
    logger.info(f"Bot is online as {bot.user}")


@bot.tree.command(name='setup', description='Configure badge, tag, and role for tracking')
@commands.has_permissions(administrator=True)
async def setup(interaction: discord.Interaction, badge: str, tag: str):
    cleanup_invalid_badges()  # 🔄 Ensure database is clean before setting up

    if len(badge) < 3:
        await interaction.response.send_message("Badge must be at least 3 characters long.", ephemeral=True)
        logger.warning(f"Setup attempt failed: Badge '{badge}' is less than 3 characters.")
        return
    
    guild_id = str(interaction.guild_id)
    cursor.execute("REPLACE INTO server_config (guild_id, role_name, badge, tag) VALUES (?, (SELECT role_name FROM server_config WHERE guild_id = ?), ?, ?)", (guild_id, guild_id, badge, tag))
    conn.commit()
    await interaction.response.send_message(f"Configuration set: Badge = {badge}, Tag = {tag}")
    logger.info(f"Configuration set for Guild {guild_id}: Badge = {badge}, Tag = {tag}")
# Register application command to set the role
@bot.tree.command(name='tagrole', description='Set the role to apply for tagged users')
@commands.has_permissions(administrator=True)
async def tagrole(interaction: discord.Interaction, role: discord.Role):
    guild_id = str(interaction.guild_id)
    cursor.execute("REPLACE INTO server_config (guild_id, role_name, badge, tag) VALUES (?, ?, (SELECT badge FROM server_config WHERE guild_id = ?), (SELECT tag FROM server_config WHERE guild_id = ?))", (guild_id, role.name, guild_id, guild_id))
    conn.commit()
    await interaction.response.send_message(f"Role set to: {role.name}. All users refreshed.")
    logger.info(f"Role set to: {role.name} for Guild {guild_id}")

# Register application command to force reload all users
@bot.tree.command(name='forcereload', description='Force a reload of all users to update roles')
@commands.has_permissions(administrator=True)
async def forcereload(interaction: discord.Interaction):
    await check_users()
    await interaction.response.send_message("Force reload complete. All users checked.")
    logger.info("Force reload completed.")

# Register application command to fetch user data by ID
@bot.tree.command(name='id', description='Fetches user data from Discord API so you can get the string for /setup')
async def id(interaction: discord.Interaction, user_id: str):
    try:
        # Enforce rate limits for this API call
        await enforce_per_second_rate_limit()  # 50 requests per second
        await enforce_invalid_request_limit()  # 10,000 invalid requests per 10 minutes
        user_data = fetch_user_data(user_id)
        formatted_data = json.dumps(user_data, indent=4)
        await interaction.response.send_message(f"```json\n{formatted_data}\n```")
        logger.info(f"Data sent for user ID: {user_id}")
    except Exception as e:
        await interaction.response.send_message(f"Failed to fetch user data: {str(e)}")
        logger.error(f"Error fetching data for user ID: {user_id} - {str(e)}")

# Main function to run the bot with auto-restart
def main():
    RESTART_DELAY = 5  # Delay in seconds before restarting after a crash
    while True:
        try:
            logger.info("Starting bot...")
            bot.run(TOKEN)
            break  # If bot.run exits normally (e.g., manual shutdown), break the loop
        except Exception as e:
            # Log the crash with full traceback
            logger.error(f"Bot crashed with error: {str(e)}")
            logger.error(f"Traceback:\n{traceback.format_exc()}")
            logger.info(f"Restarting bot in {RESTART_DELAY} seconds...")
            time.sleep(RESTART_DELAY)

# Run the bot with auto-restart
if __name__ == "__main__":
    main()
