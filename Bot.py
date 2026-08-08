"""
Discord social-media simulator bot
- Uses discord.py (v2+), aiosqlite for persistent SQLite DB.
- Slash commands for posts, reels, stories, profile, feed, explore, like, comment, share, follow, unfollow, achievements, stats, notifications, leaderboard, bio.
- Background simulation loop for simulated accounts (views, likes, comments, shares, follower growth, virality).
- Creates server channels if missing.
- Stores data in SQLite (data persists across restarts).
- All simulated accounts are explicitly labeled "SIM" in their displayed username/embeds.
"""
import os
import asyncio
import random
import math
import aiosqlite
import datetime
from collections import defaultdict
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import tasks

from dotenv import load_dotenv

load_dotenv()

# Config via env
TOKEN = os.getenv("DISCORD_TOKEN")
GUILD_ID = os.getenv("GUILD_ID")  # optional, for fast command registration
DB_PATH = os.getenv("DB_PATH", "socialsim.db")
SIMULATED_COUNT = int(os.getenv("SIMULATED_COUNT", "40"))
SIMULATION_INTERVAL = int(os.getenv("SIMULATION_INTERVAL", "15"))  # seconds between simulation ticks
STORY_EXPIRY_HOURS = int(os.getenv("STORY_EXPIRY_HOURS", "24"))

if TOKEN is None:
    raise RuntimeError("DISCORD_TOKEN must be set in environment variables or .env file")

intents = discord.Intents.default()
intents.message_content = False
intents.guilds = True
intents.members = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# If GUILD_ID provided, register commands to that guild for rapid testing
TEST_GUILD = discord.Object(int(GUILD_ID)) if GUILD_ID else None

# Utility helpers
def now_ts():
    return int(datetime.datetime.utcnow().timestamp())

def fmt_ts(ts: int):
    return datetime.datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S UTC")

def clamp(x, a, b):
    return max(a, min(b, x))

# Pre-made simulated accounts and comment templates
SIMULATED_PROFILES = [
    # (username, bio, interests, activity_level 0-1)
    ("HoopsFan_SIM", "SIM Account • Hoops highlights & stats", ["basketball", "highlights"], 0.9),
    ("MemeGoblin_SIM", "SIM Account • Memes & reactions", ["memes", "funny"], 0.8),
    ("DailyHoops_SIM", "SIM Account • Daily basketball posts", ["basketball"], 0.7),
    ("HighlightHunter_SIM", "SIM Account • I collect top plays", ["highlights", "basketball"], 0.6),
    ("DunkCentral_SIM", "SIM Account • Dunk edits & clips", ["dunks","basketball"], 0.8),
    ("NBAArchive_SIM", "SIM Account • Old-school NBA content", ["basketball","history"], 0.4),
    ("RandomViewer_SIM", "SIM Account • I watch a bit of everything", ["random"], 0.5),
    ("BallIsLife_SIM", "SIM Account • Ball is life", ["basketball"], 0.9),
    ("FourthQuarter_SIM", "SIM Account • Late-game content", ["basketball","clutch"], 0.6),
    ("TripleDouble_SIM", "SIM Account • Stat nerd", ["stats","basketball"], 0.5),
    # Extra names to reach desired count (will be randomized)
]

COMMENT_TEMPLATES = {
    "basketball": [
        "🔥 That finish!",
        "W POSED",
        "This is insane 🤯",
        "Bro cooked",
        "Massive W",
        "What a clutch play",
        "That dunk made my day",
        "Love this highlight",
    ],
    "memes": [
        "😂😂😂",
        "This slaps",
        "I lost it",
        "Mood",
        "This deserves more views",
        "Legendary",
    ],
    "highlights": [
        "Top 10 clip for sure",
        "Replay this 10x",
        "On repeat 🔁",
        "Can't believe that happened",
    ],
    "random": [
        "Interesting!",
        "Nice post",
        "Cool",
        "Didn't expect that",
    ],
    "default": [
        "🔥🔥🔥",
        "W POST",
        "This is crazy 💀",
        "Actually insane",
        "Massive W",
        "This deserves more views",
        "Nice one!",
    ],
}

# Ensure enough SIM profiles generated
def generate_sim_accounts(target):
    sims = list(SIMULATED_PROFILES)
    idx = 0
    while len(sims) < target:
        base = f"Viewer{idx}_SIM"
        sims.append((base, "SIM Account • Random viewer", ["random"], random.uniform(0.2, 0.8)))
        idx += 1
    # shuffle for variety
    random.shuffle(sims)
    return sims[:target]

SIMULATED_PROFILES = generate_sim_accounts(SIMULATED_COUNT)

# Database helper class
class Database:
    def __init__(self, path=DB_PATH):
        self.path = path
        self.db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self.db = await aiosqlite.connect(self.path)
        await self.db.execute("PRAGMA foreign_keys = ON;")
        await self._init_schema()
        await self.db.commit()

    async def _init_schema(self):
        # Create tables if not exist
        schema_statements = [
            # Users: real Discord users are tracked
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT UNIQUE,
                username TEXT,
                avatar_url TEXT,
                bio TEXT,
                created_at INTEGER DEFAULT (strftime('%s','now'))
            );
            """,
            # Simulated users
            """
            CREATE TABLE IF NOT EXISTS simulated_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sim_name TEXT UNIQUE,
                avatar_url TEXT,
                bio TEXT,
                interests TEXT, -- comma separated
                activity_level REAL,
                followers INTEGER DEFAULT 0,
                following INTEGER DEFAULT 0,
                created_at INTEGER DEFAULT (strftime('%s','now'))
            );
            """,
            # Posts
            """
            CREATE TABLE IF NOT EXISTS posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT UNIQUE,
                creator_discord_id TEXT,
                creator_name TEXT,
                creator_avatar TEXT,
                image_url TEXT,
                caption TEXT,
                location TEXT,
                hashtags TEXT,
                created_ts INTEGER,
                is_reel INTEGER DEFAULT 0,
                is_story INTEGER DEFAULT 0,
                story_expires_at INTEGER DEFAULT NULL,
                reach INTEGER DEFAULT 0,
                views INTEGER DEFAULT 0,
                likes INTEGER DEFAULT 0,
                comments INTEGER DEFAULT 0,
                shares INTEGER DEFAULT 0,
                status TEXT DEFAULT '🆕 New'
            );
            """,
            # Likes (prevents duplicates)
            """
            CREATE TABLE IF NOT EXISTS likes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                who TEXT, -- discord_id OR sim:sim_name
                ts INTEGER
            );
            """,
            # Comments
            """
            CREATE TABLE IF NOT EXISTS comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                who TEXT,
                content TEXT,
                ts INTEGER
            );
            """,
            # Views
            """
            CREATE TABLE IF NOT EXISTS views (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                who TEXT,
                ts INTEGER
            );
            """,
            # Shares
            """
            CREATE TABLE IF NOT EXISTS shares (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id TEXT,
                who TEXT,
                ts INTEGER
            );
            """,
            # Followers (both real and simulated)
            """
            CREATE TABLE IF NOT EXISTS followers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner TEXT, -- discord_id OR sim:sim_name
                follower TEXT, -- discord_id OR sim:sim_name
                ts INTEGER
            );
            """,
            # Notifications
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT,
                content TEXT,
                ts INTEGER,
                seen INTEGER DEFAULT 0
            );
            """,
            # Achievements
            """
            CREATE TABLE IF NOT EXISTS achievements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_id TEXT,
                key TEXT,
                earned_ts INTEGER
            );
            """,
            # Indexes for performance
            "CREATE INDEX IF NOT EXISTS idx_posts_postid ON posts(post_id);",
            "CREATE INDEX IF NOT EXISTS idx_likes_postid ON likes(post_id);",
            "CREATE INDEX IF NOT EXISTS idx_views_postid ON views(post_id);",
        ]
        for s in schema_statements:
            await self.db.execute(s)

    # Basic DB convenience functions used in code
    async def fetchone(self, query, params=()):
        cur = await self.db.execute(query, params)
        row = await cur.fetchone()
        await cur.close()
        return row

    async def fetchall(self, query, params=()):
        cur = await self.db.execute(query, params)
        rows = await cur.fetchall()
        await cur.close()
        return rows

    async def execute(self, query, params=()):
        cur = await self.db.execute(query, params)
        await cur.close()
        await self.db.commit()

db = Database(DB_PATH)

# Simulation settings
SIMULATION_LOCK = asyncio.Lock()

# Channel names (configurable)
CHANNELS = {
    "home": "🏠・home",
    "explore": "🔎・explore",
    "posts": "📸・posts",
    "reels": "🎬・reels",
    "stories": "⏱️・stories",
    "notifications": "🔔・notifications",
    "analytics": "📊・analytics",
    "leaderboard": "🏆・leaderboard",
    "bot_activity": "🤖・bot-activity",
}

# UI Views and Buttons
class PostButtons(discord.ui.View):
    def __init__(self, post_id: str, author_discord_id: str):
        super().__init__(timeout=None)
        self.post_id = post_id
        self.author_discord_id = author_discord_id

    @discord.ui.button(label="❤️ Like", style=discord.ButtonStyle.secondary, custom_id="like_btn")
    async def like_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_like(interaction.user, self.post_id, interaction)
        await interaction.response.defer()

    @discord.ui.button(label="💬 Comment", style=discord.ButtonStyle.secondary, custom_id="comment_btn")
    async def comment_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Open modal to collect comment
        modal = CommentModal(self.post_id)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="🔄 Share", style=discord.ButtonStyle.secondary, custom_id="share_btn")
    async def share_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await handle_share(interaction.user, self.post_id, interaction)
        await interaction.response.defer()

    @discord.ui.button(label="👤 Profile", style=discord.ButtonStyle.primary, custom_id="profile_btn")
    async def profile_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await send_profile(interaction.user, interaction, interaction.user.id)

class CommentModal(discord.ui.Modal, title="Add a comment"):
    def __init__(self, post_id: str):
        super().__init__()
        self.post_id = post_id
        self.comment = discord.ui.TextInput(label="Comment", style=discord.TextStyle.short, max_length=300)
        self.add_item(self.comment)

    async def on_submit(self, interaction: discord.Interaction):
        text = self.comment.value.strip()
        if not text:
            await interaction.response.send_message("Comment cannot be empty.", ephemeral=True)
            return
        await handle_comment(interaction.user, self.post_id, text, interaction)
        await interaction.response.send_message("Comment posted!", ephemeral=True)

# High-level actions
async def ensure_simulated_users():
    # Insert simulated accounts into DB if they don't exist
    for name, bio, interests, activity_level in SIMULATED_PROFILES:
        row = await db.fetchone("SELECT id FROM simulated_users WHERE sim_name = ?", (name,))
        if not row:
            await db.execute(
                "INSERT INTO simulated_users (sim_name, bio, interests, activity_level, followers, following) VALUES (?, ?, ?, ?, ?, ?)",
                (name, bio, ",".join(interests), float(activity_level), random.randint(10, 200), random.randint(0, 50))
            )

async def ensure_channels(guild: discord.Guild):
    # Create server channels if missing
    existing = {c.name: c for c in guild.text_channels}
    for key, name in CHANNELS.items():
        if name not in existing:
            try:
                await guild.create_text_channel(name, reason="SocialSimulator creating social channels")
                print(f"Created channel {name}")
            except Exception as e:
                print("Failed create channel", name, e)

# Helper: generate unique post_id
def make_post_id():
    return f"P{int(datetime.datetime.utcnow().timestamp())}{random.randint(1000,9999)}"

# Post embed builder
def build_post_embed(row):
    # row is a mapping per SELECT
    embed = discord.Embed(
        title=f"{row['creator_name']}",
        description=row['caption'] or "",
        timestamp=datetime.datetime.utcfromtimestamp(row['created_ts'])
    )
    # author details
    embed.set_author(name=row['creator_name'], icon_url=row['creator_avatar'] or discord.Embed.Empty)
    if row['image_url']:
        embed.set_image(url=row['image_url'])
    footer = f"Post ID: {row['post_id']} • {row['status']}"
    stats = f"❤️ {row['likes']} • 💬 {row['comments']} • 👀 {row['views']} • 🔄 {row['shares']} • Reach: {row['reach']}"
    embed.add_field(name="Stats", value=stats, inline=False)
    embed.set_footer(text=footer)
    return embed

# Low-level event handlers for likes/comments/shares/views
async def handle_like(user: discord.user.BaseUser, post_id: str, interaction: Optional[discord.Interaction]=None, actor_identifier: Optional[str]=None):
    # actor_identifier override to simulate sim:NAME
    who = actor_identifier if actor_identifier else f"discord:{user.id}"
    ts = now_ts()
    # prevent duplicate like
    existing = await db.fetchone("SELECT id FROM likes WHERE post_id=? AND who=?", (post_id, who))
    if existing:
        if interaction:
            await interaction.followup.send("Already liked.", ephemeral=True)
        return False
    await db.execute("INSERT INTO likes (post_id, who, ts) VALUES (?, ?, ?)", (post_id, who, ts))
    await db.execute("UPDATE posts SET likes = likes + 1 WHERE post_id = ?", (post_id,))
    # update reach modestly
    await db.execute("UPDATE posts SET reach = reach + ? WHERE post_id = ?", (random.randint(5, 25), post_id))
    # Notify author if a real user
    post_row = await db.fetchone("SELECT creator_discord_id FROM posts WHERE post_id = ?", (post_id,))
    if post_row and post_row[0] and post_row[0].startswith("discord:"):
        author = post_row[0].split("discord:")[1]
        await db.execute("INSERT INTO notifications (discord_id, content, ts) VALUES (?, ?, ?)", (author, f"❤️ Someone liked your post {post_id}", ts))
    return True

async def handle_comment(user: discord.user.BaseUser, post_id: str, text: str, interaction: Optional[discord.Interaction]=None, actor_identifier: Optional[str]=None):
    who = actor_identifier if actor_identifier else f"discord:{user.id}"
    ts = now_ts()
    await db.execute("INSERT INTO comments (post_id, who, content, ts) VALUES (?, ?, ?, ?)", (post_id, who, text, ts))
    await db.execute("UPDATE posts SET comments = comments + 1 WHERE post_id = ?", (post_id,))
    await db.execute("UPDATE posts SET reach = reach + ? WHERE post_id = ?", (random.randint(10, 40), post_id))
    post_row = await db.fetchone("SELECT creator_discord_id FROM posts WHERE post_id = ?", (post_id,))
    if post_row and post_row[0] and post_row[0].startswith("discord:"):
        author = post_row[0].split("discord:")[1]
        await db.execute("INSERT INTO notifications (discord_id, content, ts) VALUES (?, ?, ?)", (author, f"💬 Someone commented on your post {post_id}", ts))
    return True

async def handle_view(actor_identifier: str, post_id: str):
    ts = now_ts()
    await db.execute("INSERT INTO views (post_id, who, ts) VALUES (?, ?, ?)", (post_id, actor_identifier, ts))
    await db.execute("UPDATE posts SET views = views + 1 WHERE post_id = ?", (post_id,))
    # small reach bump
    await db.execute("UPDATE posts SET reach = reach + ? WHERE post_id = ?", (random.randint(1,7), post_id))

async def handle_share(user: discord.user.BaseUser, post_id: str, interaction: Optional[discord.Interaction]=None, actor_identifier: Optional[str]=None):
    who = actor_identifier if actor_identifier else f"discord:{user.id}"
    ts = now_ts()
    await db.execute("INSERT INTO shares (post_id, who, ts) VALUES (?, ?, ?)", (post_id, who, ts))
    await db.execute("UPDATE posts SET shares = shares + 1 WHERE post_id = ?", (post_id,))
    # shares have larger effect on reach
    await db.execute("UPDATE posts SET reach = reach + ? WHERE post_id = ?", (random.randint(30, 200), post_id))
    post_row = await db.fetchone("SELECT creator_discord_id FROM posts WHERE post_id = ?", (post_id,))
    if post_row and post_row[0] and post_row[0].startswith("discord:"):
        author = post_row[0].split("discord:")[1]
        await db.execute("INSERT INTO notifications (discord_id, content, ts) VALUES (?, ?, ?)", (author, f"🔄 Someone shared your post {post_id}", ts))
    return True

async def add_follower(owner_identifier: str, follower_identifier: str):
    ts = now_ts()
    exists = await db.fetchone("SELECT id FROM followers WHERE owner=? AND follower=?", (owner_identifier, follower_identifier))
    if exists:
        return False
    await db.execute("INSERT INTO followers (owner, follower, ts) VALUES (?, ?, ?)", (owner_identifier, follower_identifier, ts))
    # increment follower count on simulated user if applicable
    if owner_identifier.startswith("sim:"):
        await db.execute("UPDATE simulated_users SET followers = followers + 1 WHERE sim_name = ?", (owner_identifier.split("sim:")[1],))
    return True

async def remove_follower(owner_identifier: str, follower_identifier: str):
    await db.execute("DELETE FROM followers WHERE owner=? AND follower=?", (owner_identifier, follower_identifier))
    if owner_identifier.startswith("sim:"):
        await db.execute("UPDATE simulated_users SET followers = followers - 1 WHERE sim_name = ?", (owner_identifier.split("sim:")[1],))
    return True

# Virality algorithm
def compute_virality_score(views, likes, comments, shares, reach, created_ts, follower_count):
    age_seconds = max(1, now_ts() - created_ts)
    # engagement rate approximate
    engagement = (likes + comments * 2 + shares * 3) / max(1, views)
    velocity = (likes + comments + shares) / max(1, age_seconds)  # actions per second
    follower_factor = math.log1p(follower_count)
    randomness = random.uniform(0.8, 1.2)
    score = (engagement * 1000) + (math.log1p(views) * 2) + (velocity * 5000) + follower_factor * 10 + math.log1p(reach) * 0.5
    score *= randomness
    return score

def map_score_to_status(score):
    if score < 10:
        return "🆕 New"
    if score < 100:
        return "📈 Rising"
    if score < 500:
        return "🔥 Trending"
    if score < 2000:
        return "🚀 Viral"
    return "💎 Mega Viral"

# Background simulation loop
@tasks.loop(seconds=SIMULATION_INTERVAL)
async def simulation_tick():
    async with SIMULATION_LOCK:
        try:
            # Choose some recent posts to act on
            posts = await db.fetchall("SELECT post_id, created_ts, reach, views, likes, comments, shares FROM posts ORDER BY created_ts DESC LIMIT 50")
            if not posts:
                return
            simulated_list = await db.fetchall("SELECT sim_name, interests, activity_level, followers FROM simulated_users")
            sim_objs = []
            for s in simulated_list:
                sim_objs.append({
                    "sim_name": s[0],
                    "interests": s[1].split(",") if s[1] else [],
                    "activity_level": float(s[2]),
                    "followers": int(s[3] or 0)
                })
            # For each post, decide some simulated viewers this tick
            for row in posts:
                post_id = row[0]
                created_ts = int(row[1])
                reach = int(row[2] or 0)
                views = int(row[3] or 0)
                likes = int(row[4] or 0)
                comments = int(row[5] or 0)
                shares = int(row[6] or 0)
                # determine interest tag heuristics: peek caption/hashtags
                pinfo = await db.fetchone("SELECT caption, hashtags, creator_discord_id FROM posts WHERE post_id = ?", (post_id,))
                caption = pinfo[0] or ""
                hashtags = pinfo[1] or ""
                creator = pinfo[2] or ""
                # simple category detection
                category_scores = defaultdict(float)
                content_text = (caption + " " + hashtags).lower()
                if any(k in content_text for k in ["dunk","basket","nba","hoop","hoops","court","finis"]):
                    category_scores["basketball"] += 1.0
                if any(k in content_text for k in ["meme","lol","funny","joke"]):
                    category_scores["memes"] += 1.0
                if any(k in content_text for k in ["highlight","clip","best"]):
                    category_scores["highlights"] += 1.0
                # fallback
                primary_category = "random"
                if category_scores:
                    primary_category = max(category_scores.items(), key=lambda x: x[1])[0]
                # select random subset of sims influenced by activity and interest
                for sim in sim_objs:
                    # base probability
                    prob = sim["activity_level"] * 0.05  # baseline per tick
                    # if interest matches, boost probability
                    if primary_category in sim["interests"]:
                        prob *= 3.0
                    # if trending/high reach, more discovery
                    if reach > 1000:
                        prob *= 1.5
                    # convert to probability clamp
                    prob = clamp(prob, 0.001, 0.9)
                    if random.random() < prob:
                        sim_who = f"sim:{sim['sim_name']}"
                        # view
                        await handle_view(sim_who, post_id)
                        # maybe like
                        if random.random() < (0.15 * sim["activity_level"]):
                            # check duplicate like
                            exists = await db.fetchone("SELECT id FROM likes WHERE post_id=? AND who=?", (post_id, sim_who))
                            if not exists:
                                await handle_like(None, post_id, actor_identifier=sim_who)
                        # maybe comment
                        if random.random() < (0.03 * sim["activity_level"]):
                            # pick a template based on interest
                            templates = COMMENT_TEMPLATES.get(primary_category, COMMENT_TEMPLATES["default"])
                            comment_text = random.choice(templates)
                            await handle_comment(None, post_id, comment_text, actor_identifier=sim_who)
                        # maybe share
                        if random.random() < (0.01 * sim["activity_level"]):
                            await handle_share(None, post_id, actor_identifier=sim_who)
                        # maybe follow the creator if not already following
                        if creator:
                            owner_identifier = creator
                            follower_identifier = sim_who
                            follow_prob = 0.005 * sim["activity_level"]
                            # boost follow chance if creator's post is getting lots of engagement
                            if likes + comments + shares > 50:
                                follow_prob *= 3
                            if random.random() < follow_prob:
                                await add_follower(owner_identifier, follower_identifier)
                # Recompute virality
                # Need creator followers for factor
                creator_followers = 0
                if creator:
                    if creator.startswith("discord:"):
                        # real user: count followers rows where owner = creator
                        rows = await db.fetchall("SELECT follower FROM followers WHERE owner = ?", (creator,))
                        creator_followers = len(rows)
                    elif creator.startswith("sim:"):
                        # get simulated followers
                        rowf = await db.fetchone("SELECT followers FROM simulated_users WHERE sim_name = ?", (creator.split("sim:")[1],))
                        creator_followers = int(rowf[0]) if rowf else 0
                # fetch latest counts
                latest = await db.fetchone("SELECT views, likes, comments, shares, reach, created_ts FROM posts WHERE post_id = ?", (post_id,))
                if not latest:
                    continue
                v, l, c, s_, rch, created = int(latest[0]), int(latest[1]), int(latest[2]), int(latest[3]), int(latest[4]), int(latest[5])
                score = compute_virality_score(v, l, c, s_, rch, created, creator_followers)
                new_status = map_score_to_status(score)
                # update if changed
                if new_status != await db.fetchone("SELECT status FROM posts WHERE post_id = ?", (post_id,))[0]:
                    await db.execute("UPDATE posts SET status = ? WHERE post_id = ?", (new_status, post_id))
                    # If trending/viral, send notification to owner if real user
                    if new_status in ("🔥 Trending", "🚀 Viral", "💎 Mega Viral"):
                        post_r = await db.fetchone("SELECT creator_discord_id FROM posts WHERE post_id = ?", (post_id,))
                        if post_r and post_r[0] and post_r[0].startswith("discord:"):
                            owner = post_r[0].split("discord:")[1]
                            await db.execute("INSERT INTO notifications (discord_id, content, ts) VALUES (?, ?, ?)", (owner, f"🚨 Your post {post_id} is now {new_status}!", now_ts()))
                            # follower growth proportional to status
                            base_growth = {"🔥 Trending": (5, 20), "🚀 Viral": (50, 300), "💎 Mega Viral": (500, 5000)}.get(new_status, (0,0))
                            growth = random.randint(*base_growth)
                            # create that many follow events distributed among sims (approx)
                            sims_to_gain = random.sample([s["sim_name"] for s in sim_objs], k=min(len(sim_objs), clamp(growth//10, 1, len(sim_objs))))
                            for sim_name in sims_to_gain:
                                await add_follower(f"discord:{owner}", f"sim:{sim_name}")
        except Exception as e:
            print("Simulation tick error:", e)

# Utility: create post message in posts channel
async def post_to_channel(guild: discord.Guild, post_id: str, creator_member: discord.Member, image_url: str, caption: str, hashtags: Optional[str], location: Optional[str], is_reel=False, is_story=False, story_expires_at: Optional[int]=None):
    posts_channel = discord.utils.get(guild.text_channels, name=CHANNELS["posts"])
    if is_reel:
        posts_channel = discord.utils.get(guild.text_channels, name=CHANNELS["reels"]) or posts_channel
    if is_story:
        posts_channel = discord.utils.get(guild.text_channels, name=CHANNELS["stories"]) or posts_channel
    embed = discord.Embed(title=f"{creator_member.display_name}", description=caption or "", timestamp=datetime.datetime.utcfromtimestamp(now_ts()))
    embed.set_author(name=creator_member.display_name, icon_url=str(creator_member.display_avatar.url) if creator_member.display_avatar else None)
    if image_url:
        embed.set_image(url=image_url)
    footer = f"Post ID: {post_id}"
    embed.add_field(name="Stats", value="❤️ 0 • 💬 0 • 👀 0 • 🔄 0 • Reach: 0", inline=False)
    if hashtags:
        embed.add_field(name="Hashtags", value=hashtags, inline=False)
    if location:
        embed.add_field(name="Location", value=location, inline=False)
    embed.set_footer(text=footer)
    view = PostButtons(post_id, f"discord:{creator_member.id}")
    try:
        await posts_channel.send(embed=embed, view=view)
    except Exception:
        # fallback to guild owner DM if cannot post
        owner = guild.owner
        if owner:
            await owner.send(f"Could not post to channel {posts_channel.name}. Here is your post {post_id}")

# Profile view sender
async def send_profile(user: discord.User, interaction: Optional[discord.Interaction], target_discord_id: Optional[int]=None):
    # target_discord_id: if None -> show user's own profile
    if target_discord_id is None:
        target_discord_id = user.id
    discord_id_str = f"discord:{target_discord_id}"
    # collect stats
    posts = await db.fetchall("SELECT post_id, likes, views, reach, status FROM posts WHERE creator_discord_id = ? ORDER BY created_ts DESC", (discord_id_str,))
    total_posts = len(posts)
    total_likes = sum([int(p[1]) for p in posts]) if posts else 0
    total_views = sum([int(p[2]) for p in posts]) if posts else 0
    total_reach = sum([int(p[3]) for p in posts]) if posts else 0
    followers = await db.fetchall("SELECT follower FROM followers WHERE owner = ?", (discord_id_str,))
    following = await db.fetchall("SELECT owner FROM followers WHERE follower = ?", (discord_id_str,))
    following_count = len(following)
    followers_count = len(followers)
    # achievements
    achs = await db.fetchall("SELECT key, earned_ts FROM achievements WHERE discord_id = ?", (discord_id_str,))
    embed = discord.Embed(title=f"{user.display_name}'s Profile", description=f"{user.display_name}", timestamp=datetime.datetime.utcnow())
    embed.set_thumbnail(url=str(user.display_avatar.url) if user.display_avatar else None)
    embed.add_field(name="Bio", value=(await db.fetchone("SELECT bio FROM users WHERE discord_id = ?", (discord_id_str,)) or ("",))[0] or "No bio set", inline=False)
    embed.add_field(name="Followers / Following", value=f"{followers_count} • {following_count}")
    embed.add_field(name="Posts", value=str(total_posts))
    embed.add_field(name="Total Likes", value=str(total_likes))
    embed.add_field(name="Total Views", value=str(total_views))
    embed.add_field(name="Total Reach", value=str(total_reach))
    embed.add_field(name="Achievements", value=", ".join([a[0] for a in achs]) if achs else "None", inline=False)
    if interaction:
        await interaction.response.send_message(embed=embed, ephemeral=True)
    else:
        # fallback to DM
        try:
            await user.send(embed=embed)
        except Exception:
            pass

# Command implementations
@tree.command(name="post", description="Create a new image post")
@app_commands.describe(image="Image attachment (optional)", caption="Caption text", location="Location", hashtags="Comma-separated hashtags")
async def cmd_post(interaction: discord.Interaction, image: Optional[discord.Attachment], caption: Optional[str], location: Optional[str], hashtags: Optional[str]):
    await interaction.response.defer(thinking=True)
    # store user in DB if new
    user = interaction.user
    discord_id_str = f"discord:{user.id}"
    row = await db.fetchone("SELECT id FROM users WHERE discord_id = ?", (discord_id_str,))
    if not row:
        avatar = str(user.display_avatar.url) if user.display_avatar else None
        await db.execute("INSERT INTO users (discord_id, username, avatar_url, bio) VALUES (?, ?, ?, ?)", (discord_id_str, user.display_name, avatar, ""))
    # upload image: we store attachment.url as image_url
    image_url = None
    if image:
        image_url = image.url
    post_id = make_post_id()
    ts = now_ts()
    await db.execute(
        "INSERT INTO posts (post_id, creator_discord_id, creator_name, creator_avatar, image_url, caption, location, hashtags, created_ts, is_reel, is_story) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, 0)",
        (post_id, discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, image_url, caption or "", location or "", hashtags or "", ts)
    )
    # send to channel
    guild = interaction.guild
    if guild:
        await post_to_channel(guild, post_id, interaction.user, image_url, caption or "", hashtags, location)
    await interaction.followup.send(f"Posted! Post ID: {post_id}", ephemeral=True)

@tree.command(name="reel", description="Create a new reel (video)")
@app_commands.describe(video="Video attachment", caption="Caption text", hashtags="Comma-separated hashtags")
async def cmd_reel(interaction: discord.Interaction, video: discord.Attachment, caption: Optional[str], hashtags: Optional[str]):
    await interaction.response.defer(thinking=True)
    user = interaction.user
    discord_id_str = f"discord:{user.id}"
    row = await db.fetchone("SELECT id FROM users WHERE discord_id = ?", (discord_id_str,))
    if not row:
        await db.execute("INSERT INTO users (discord_id, username, avatar_url, bio) VALUES (?, ?, ?, ?)", (discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, ""))
    video_url = video.url
    post_id = make_post_id()
    ts = now_ts()
    await db.execute(
        "INSERT INTO posts (post_id, creator_discord_id, creator_name, creator_avatar, image_url, caption, hashtags, created_ts, is_reel) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)",
        (post_id, discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, video_url, caption or "", hashtags or "", ts)
    )
    guild = interaction.guild
    if guild:
        await post_to_channel(guild, post_id, interaction.user, video_url, caption or "", hashtags, None, is_reel=True)
    await interaction.followup.send(f"Reel posted! Post ID: {post_id}", ephemeral=True)

@tree.command(name="story", description="Create a story that expires")
@app_commands.describe(attachment="Image or video", text="Optional text for story")
async def cmd_story(interaction: discord.Interaction, attachment: Optional[discord.Attachment], text: Optional[str]):
    await interaction.response.defer(thinking=True)
    user = interaction.user
    discord_id_str = f"discord:{user.id}"
    if not await db.fetchone("SELECT id FROM users WHERE discord_id = ?", (discord_id_str,)):
        await db.execute("INSERT INTO users (discord_id, username, avatar_url, bio) VALUES (?, ?, ?, ?)", (discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, ""))
    image_url = attachment.url if attachment else None
    post_id = make_post_id()
    created = now_ts()
    expires = created + STORY_EXPIRY_HOURS * 3600
    await db.execute(
        "INSERT INTO posts (post_id, creator_discord_id, creator_name, creator_avatar, image_url, caption, created_ts, is_story, story_expires_at) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?)",
        (post_id, discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, image_url, text or "", created, expires)
    )
    guild = interaction.guild
    if guild:
        await post_to_channel(guild, post_id, interaction.user, image_url, text or "", None, None, is_story=True, story_expires_at=expires)
    await interaction.followup.send(f"Story posted! It will expire in {STORY_EXPIRY_HOURS} hours. Post ID: {post_id}", ephemeral=True)

@tree.command(name="stories", description="View your active stories")
async def cmd_stories(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)
    user = interaction.user
    discord_id_str = f"discord:{user.id}"
    nowt = now_ts()
    rows = await db.fetchall("SELECT post_id, image_url, caption, story_expires_at FROM posts WHERE creator_discord_id = ? AND is_story = 1 AND (story_expires_at IS NULL OR story_expires_at > ?) ORDER BY created_ts DESC", (discord_id_str, nowt))
    if not rows:
        await interaction.followup.send("No active stories.", ephemeral=True)
        return
    embeds = []
    for r in rows:
        pid, img, cap, exp = r
        embed = discord.Embed(title=f"Story • {interaction.user.display_name}", description=cap or "", timestamp=datetime.datetime.utcfromtimestamp(now_ts()))
        if img:
            embed.set_image(url=img)
        if exp:
            embed.set_footer(text=f"Expires at {fmt_ts(int(exp))}")
        embeds.append(embed)
    for e in embeds:
        await interaction.followup.send(embed=e, ephemeral=True)

@tree.command(name="profile", description="Show profile")
@app_commands.describe(target="User to view (optional)")
async def cmd_profile(interaction: discord.Interaction, target: Optional[discord.Member]):
    await interaction.response.defer(thinking=True, ephemeral=True)
    target_id = target.id if target else None
    await send_profile(interaction.user, interaction, target_id)

@tree.command(name="bio", description="Set your profile bio")
@app_commands.describe(bio="New bio text")
async def cmd_bio(interaction: discord.Interaction, bio: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    user = interaction.user
    discord_id_str = f"discord:{user.id}"
    if not await db.fetchone("SELECT id FROM users WHERE discord_id = ?", (discord_id_str,)):
        await db.execute("INSERT INTO users (discord_id, username, avatar_url, bio) VALUES (?, ?, ?, ?)", (discord_id_str, user.display_name, str(user.display_avatar.url) if user.display_avatar else None, bio))
    else:
        await db.execute("UPDATE users SET bio = ? WHERE discord_id = ?", (bio, discord_id_str))
    await interaction.followup.send("Bio updated.", ephemeral=True)

@tree.command(name="feed", description="Show a ranked feed")
async def cmd_feed(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    # Fetch recent posts and compute simple ranking based on virality score
    rows = await db.fetchall("SELECT post_id, created_ts, views, likes, comments, shares, reach FROM posts ORDER BY created_ts DESC LIMIT 200")
    ranked = []
    for r in rows:
        pid, created, views, likes, comments, shares, reach = r
        # estimate creator followers
        creator = (await db.fetchone("SELECT creator_discord_id FROM posts WHERE post_id = ?", (pid,)))[0]
        followers = 0
        if creator and creator.startswith("discord:"):
            followers = len(await db.fetchall("SELECT follower FROM followers WHERE owner = ?", (creator,)))
        elif creator and creator.startswith("sim:"):
            f = await db.fetchone("SELECT followers FROM simulated_users WHERE sim_name = ?", (creator.split("sim:")[1],))
            followers = int(f[0]) if f else 0
        score = compute_virality_score(int(views), int(likes), int(comments), int(shares), int(reach), int(created), followers)
        ranked.append((score, pid))
    ranked.sort(reverse=True, key=lambda x: x[0])
    # choose top 10 with a bit of randomness to let older posts surface
    top = [pid for (_, pid) in ranked[:15]]
    if not top:
        await interaction.followup.send("No posts yet. Make the first post!", ephemeral=True)
        return
    # send embeds for top 5
    guild = interaction.guild
    for pid in top[:5]:
        pr = await db.fetchone("SELECT post_id, creator_name, creator_avatar, image_url, caption, created_ts, likes, comments, views, shares, reach, status FROM posts WHERE post_id = ?", (pid,))
        if not pr:
            continue
        row = {
            "post_id": pr[0],
            "creator_name": pr[1],
            "creator_avatar": pr[2],
            "image_url": pr[3],
            "caption": pr[4],
            "created_ts": pr[5],
            "likes": pr[6],
            "comments": pr[7],
            "views": pr[8],
            "shares": pr[9],
            "reach": pr[10],
            "status": pr[11],
        }
        embed = build_post_embed(row)
        view = PostButtons(row["post_id"], None)
        await interaction.followup.send(embed=embed, view=view)
    await interaction.followup.send("End of feed.", ephemeral=True)

@tree.command(name="explore", description="Explore random and trending content")
async def cmd_explore(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    # Get a mixture of trending and random older posts
    trending = await db.fetchall("SELECT post_id FROM posts WHERE status IN ('🔥 Trending','🚀 Viral','💎 Mega Viral') ORDER BY reach DESC LIMIT 10")
    randoms = await db.fetchall("SELECT post_id FROM posts ORDER BY RANDOM() LIMIT 10")
    choices = [r[0] for r in trending] + [r[0] for r in randoms]
    if not choices:
        await interaction.followup.send("No content to explore yet.", ephemeral=True)
        return
    # send 5 items
    for pid in random.sample(choices, k=min(5, len(choices))):
        pr = await db.fetchone("SELECT post_id, creator_name, creator_avatar, image_url, caption, created_ts, likes, comments, views, shares, reach, status FROM posts WHERE post_id = ?", (pid,))
        if not pr:
            continue
        row = {
            "post_id": pr[0],
            "creator_name": pr[1],
            "creator_avatar": pr[2],
            "image_url": pr[3],
            "caption": pr[4],
            "created_ts": pr[5],
            "likes": pr[6],
            "comments": pr[7],
            "views": pr[8],
            "shares": pr[9],
            "reach": pr[10],
            "status": pr[11],
        }
        embed = build_post_embed(row)
        view = PostButtons(row["post_id"], None)
        await interaction.followup.send(embed=embed, view=view)
    await interaction.followup.send("Explore complete.", ephemeral=True)

@tree.command(name="like", description="Like a post")
@app_commands.describe(post_id="Post ID to like")
async def cmd_like(interaction: discord.Interaction, post_id: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    ok = await handle_like(interaction.user, post_id, interaction)
    if ok:
        await interaction.followup.send("Liked!", ephemeral=True)
    else:
        await interaction.followup.send("Already liked or error.", ephemeral=True)

@tree.command(name="comment", description="Comment on a post")
@app_commands.describe(post_id="Post ID", text="Comment text")
async def cmd_comment(interaction: discord.Interaction, post_id: str, text: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    await handle_comment(interaction.user, post_id, text, interaction)
    await interaction.followup.send("Comment posted.", ephemeral=True)

@tree.command(name="share", description="Share a post")
@app_commands.describe(post_id="Post ID to share")
async def cmd_share(interaction: discord.Interaction, post_id: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    await handle_share(interaction.user, post_id, interaction)
    await interaction.followup.send("Shared! (simulated)", ephemeral=True)

@tree.command(name="follow", description="Follow a creator (username)")
@app_commands.describe(username="Creator username (display name) to follow")
async def cmd_follow(interaction: discord.Interaction, username: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    # find a matching creator in posts/users
    # prefer exact match among users table
    row = await db.fetchone("SELECT discord_id FROM users WHERE username = ?", (username,))
    if not row:
        await interaction.followup.send("User not found.", ephemeral=True)
        return
    owner = row[0]
    follower = f"discord:{interaction.user.id}"
    added = await add_follower(owner, follower)
    if added:
        await interaction.followup.send(f"You followed {username}.", ephemeral=True)
    else:
        await interaction.followup.send("Already following.", ephemeral=True)

@tree.command(name="unfollow", description="Unfollow a creator")
@app_commands.describe(username="Creator username (display name) to unfollow")
async def cmd_unfollow(interaction: discord.Interaction, username: str):
    await interaction.response.defer(thinking=True, ephemeral=True)
    row = await db.fetchone("SELECT discord_id FROM users WHERE username = ?", (username,))
    if not row:
        await interaction.followup.send("User not found.", ephemeral=True)
        return
    owner = row[0]
    follower = f"discord:{interaction.user.id}"
    await remove_follower(owner, follower)
    await interaction.followup.send(f"Unfollowed {username}.", ephemeral=True)

@tree.command(name="achievements", description="View your achievements")
async def cmd_achievements(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)
    discord_id_str = f"discord:{interaction.user.id}"
    rows = await db.fetchall("SELECT key, earned_ts FROM achievements WHERE discord_id = ?", (discord_id_str,))
    if not rows:
        await interaction.followup.send("No achievements yet.", ephemeral=True)
        return
    text = "\n".join([f"{r[0]} — {fmt_ts(int(r[1]))}" for r in rows])
    await interaction.followup.send(f"Achievements:\n{text}", ephemeral=True)

@tree.command(name="stats", description="Analytics for your account")
async def cmd_stats(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)
    discord_id_str = f"discord:{interaction.user.id}"
    posts = await db.fetchall("SELECT post_id, views, likes, comments, shares, reach, status FROM posts WHERE creator_discord_id = ?", (discord_id_str,))
    total_posts = len(posts)
    total_views = sum([int(p[1]) for p in posts]) if posts else 0
    total_likes = sum([int(p[2]) for p in posts]) if posts else 0
    total_comments = sum([int(p[3]) for p in posts]) if posts else 0
    total_shares = sum([int(p[4]) for p in posts]) if posts else 0
    total_reach = sum([int(p[5]) for p in posts]) if posts else 0
    best_liked = max(posts, key=lambda x: x[2])[0] if posts else "N/A"
    most_viewed = max(posts, key=lambda x: x[1])[0] if posts else "N/A"
    viral_posts = [p[0] for p in posts if p[6] in ("🚀 Viral", "💎 Mega Viral")]
    embed = discord.Embed(title="Analytics", timestamp=datetime.datetime.utcnow())
    embed.add_field(name="Total posts", value=str(total_posts))
    embed.add_field(name="Total views", value=str(total_views))
    embed.add_field(name="Total likes", value=str(total_likes))
    embed.add_field(name="Total comments", value=str(total_comments))
    embed.add_field(name="Total shares", value=str(total_shares))
    embed.add_field(name="Total reach", value=str(total_reach))
    embed.add_field(name="Best liked post", value=str(best_liked))
    embed.add_field(name="Most viewed post", value=str(most_viewed))
    embed.add_field(name="Viral posts", value=", ".join(viral_posts) if viral_posts else "None")
    await interaction.followup.send(embed=embed, ephemeral=True)

@tree.command(name="notifications", description="Show your notifications")
async def cmd_notifications(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True, ephemeral=True)
    discord_id_str = f"discord:{interaction.user.id}"
    rows = await db.fetchall("SELECT id, content, ts, seen FROM notifications WHERE discord_id = ? ORDER BY ts DESC LIMIT 25", (discord_id_str,))
    if not rows:
        await interaction.followup.send("No notifications.", ephemeral=True)
        return
    text = []
    for r in rows:
        seen = "✅" if r[3] else "🔔"
        text.append(f"{seen} {fmt_ts(r[2])} • {r[1]}")
    # mark them as seen
    await db.execute("UPDATE notifications SET seen = 1 WHERE discord_id = ?", (discord_id_str,))
    await interaction.followup.send("\n".join(text), ephemeral=True)

@tree.command(name="leaderboard", description="Show leaderboard")
async def cmd_leaderboard(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)
    # For leaderboard we will rank by followers primarily and include simulated accounts and real user
    sim_rows = await db.fetchall("SELECT sim_name, followers FROM simulated_users ORDER BY followers DESC LIMIT 20")
    # fetch real users followers count
    users = await db.fetchall("SELECT discord_id, username FROM users")
    user_ranks = []
    for u in users:
        owner = u[0]
        followers = await db.fetchall("SELECT follower FROM followers WHERE owner = ?", (owner,))
        user_ranks.append((u[1], len(followers)))
    # combine
    combined = []
    for s in sim_rows:
        combined.append((s[0], s[1]))
    for u in user_ranks:
        combined.append((u[0], u[1]))
    combined.sort(key=lambda x: x[1], reverse=True)
    # present top 10
    text = []
    for i, (name, fol) in enumerate(combined[:10], start=1):
        text.append(f"{i}. {name} — {fol} followers")
    await interaction.followup.send("Leaderboard:\n" + "\n".join(text), ephemeral=False)

# Bot events
@client.event
async def on_ready():
    print(f"Logged in as {client.user} (ID: {client.user.id})")
    await db.connect()
    await ensure_simulated_users()
    # register commands (guild if testing)
    if TEST_GUILD:
        await tree.sync(guild=TEST_GUILD)
        print("Commands synced to test guild")
    else:
        await tree.sync()
        print("Global commands synced")
    # ensure channels in all guilds the bot is in
    for g in client.guilds:
        await ensure_channels(g)
    # start simulation
    if not simulation_tick.is_running():
        simulation_tick.start()
    print("Simulation started.")

@client.event
async def on_guild_join(guild: discord.Guild):
    # create channels on joining a guild
    await ensure_channels(guild)

# Periodic cleanup of expired stories
@tasks.loop(minutes=15)
async def cleanup_stories():
    nowt = now_ts()
    expired = await db.fetchall("SELECT post_id FROM posts WHERE is_story = 1 AND story_expires_at IS NOT NULL AND story_expires_at <= ?", (nowt,))
    for r in expired:
        pid = r[0]
        await db.execute("DELETE FROM posts WHERE post_id = ?", (pid,))

cleanup_stories.start()

# Start the client
def main():
    try:
        client.run(TOKEN)
    except Exception as e:
        print("Bot failed to start:", e)

if __name__ == "__main__":
    main()
