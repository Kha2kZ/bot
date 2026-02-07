import nest_asyncio
nest_asyncio.apply()
import time
import threading

# Thread để ping runtime
def keep_alive():
    while True:
        print("⏱️ Tai Xiu Bot Still alive")
        time.sleep(60)

t = threading.Thread(target=keep_alive)
t.start()

import discord
from discord.ext import commands
import random
import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional
from logging_setup import setup_logging

# Setup logging
setup_logging()
logger = logging.getLogger(__name__)

class TaiXiuBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        intents.guilds = True
        intents.guild_messages = True

        super().__init__(
            command_prefix='?',
            intents=intents,
            help_command=None
        )

        # Storage attributes
        self.overunder_games = {}
        self.overunder_autocycle = {}
        self.user_cash_memory = {}
        self._daily_locks = {}
        self.manual_win_result = None

        # File-based backup system
        self.backup_file_path = "user_cash_backup.json"
        self._load_backup_data()

    async def setup_hook(self):
        # Start backup task
        self.backup_task = self.loop.create_task(self._backup_data_loop())
        logger.info("Tai Xiu Bot initialized and backup loop started")

    def _load_backup_data(self):
        """Load user cash data from backup file on startup"""
        try:
            if os.path.exists(self.backup_file_path):
                with open(self.backup_file_path, 'r', encoding='utf-8') as f:
                    backup_data = json.load(f)
                    raw_user_data = backup_data.get('user_cash_memory', {})

                    # Load into memory
                    loaded_count = 0
                    for key, data in raw_user_data.items():
                        processed_data = data.copy()

                        # Convert last_daily string back to date object
                        if 'last_daily' in processed_data and processed_data['last_daily']:
                            try:
                                if isinstance(processed_data['last_daily'], str):
                                    processed_data['last_daily'] = datetime.strptime(processed_data['last_daily'], '%Y-%m-%d').date()
                            except (ValueError, TypeError):
                                processed_data['last_daily'] = None

                        self.user_cash_memory[key] = processed_data
                        loaded_count += 1

                    logger.info(f"Loaded backup data for {loaded_count} users from {self.backup_file_path}")
            else:
                logger.info("No backup file found, starting with empty memory")
        except Exception as e:
            logger.error(f"Error loading backup data: {e}")
    # === CASH SYSTEM HELPER METHODS ===
    def _get_user_cash(self, guild_id, user_id):
        """Get user's cash amount and daily streak info"""
        connection = self._get_db_connection()
        if not connection:
            # Use in-memory storage when database isn't available
            key = f"{guild_id}_{user_id}"
            if key in self.user_cash_memory:
                data = self.user_cash_memory[key]
                return data.get('cash', 1000), data.get('last_daily'), data.get('daily_streak', 0)
            else:
                # Give new users some starting cash
                return 1000, None, 0

        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    "SELECT cash, last_daily, daily_streak FROM user_cash WHERE guild_id = %s AND user_id = %s",
                    (str(guild_id), str(user_id))
                )
                result = cursor.fetchone()
                if result:
                    return result[0], result[1], result[2]
                else:
                    # Create new user with starting cash instead of returning 0
                    cursor.execute(
                        "INSERT INTO user_cash (guild_id, user_id, cash) VALUES (%s, %s, %s)",
                        (str(guild_id), str(user_id), 1000)
                    )
                    connection.commit()
                    return 1000, None, 0
        except Exception as e:
            logger.error(f"Error getting user cash: {e}")
            return 0, None, 0
        finally:
            connection.close()

    def _update_user_cash(self, guild_id, user_id, cash_amount, last_daily=None, daily_streak=None):
        """Update user's cash amount and daily streak"""
        if not self.db_connection:
            return False

        try:
            with self.db_connection.cursor() as cursor:
                if last_daily is not None and daily_streak is not None:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash, last_daily, daily_streak) 
                           VALUES (%s, %s, %s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = %s, last_daily = %s, daily_streak = %s""",
                        (str(guild_id), str(user_id), cash_amount, last_daily, daily_streak,
                         cash_amount, last_daily, daily_streak)
                    )
                else:
                    cursor.execute(
                        """INSERT INTO user_cash (guild_id, user_id, cash) 
                           VALUES (%s, %s, %s) 
                           ON CONFLICT (guild_id, user_id) 
                           DO UPDATE SET cash = user_cash.cash + %s""",
                        (str(guild_id), str(user_id), cash_amount, cash_amount)
                    )
                self.db_connection.commit()
                return True
        except Exception as e:
            logger.error(f"Error updating user cash: {e}")
            return False

    def _calculate_daily_reward(self, streak):
        """Calculate daily reward based on streak (streak=1 is first day)"""
        base_reward = 1000
        if streak <= 1:
            return base_reward  # First day = 1000 cash
        elif streak == 2:
            return 1200  # Second consecutive day
        elif streak == 3:
            return 1500  # Third consecutive day
        else:
            # Continue increasing by 400 per day after day 3
            return 1500 + (400 * (streak - 3))

    # === CASH SYSTEM COMMANDS ===
    @bot.command(name='money')
    async def show_money(ctx):
        """Show user's current money balance"""
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)

        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)

        embed = discord.Embed(
            title="💰 Thông tin tài khoản",
            description=f"**{ctx.author.mention}** - Chi tiết tài khoản của bạn",
            color=0x00ff88
        )
        embed.add_field(
            name="💎 Tài sản hiện tại",
            value=f"**{current_cash:,} VND**",
            inline=True
        )
        embed.add_field(
            name="🔥 Chuỗi ngày liên tiếp",
            value=f"**{streak} ngày**",
            inline=True
        )
        if last_daily:
            embed.add_field(
                name="📅 Lần check-in cuối cùng",
                value=f"**{last_daily}**",
                inline=True
            )
        embed.set_footer(text="Sử dụng ?daily để check-in và nhận thưởng hàng ngày! 🎁")
        await ctx.send(embed=embed)

    # === DAILY REWARD COMMAND ===
    @bot.command(name='daily')
    async def daily_reward(ctx):
        """Claim daily reward with streak bonus"""
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)
        today = datetime.utcnow().date()
        # Use atomic function to prevent race conditions and multiple earnings
        result = await bot._claim_daily_reward(guild_id, user_id, today)

        # Check if already claimed today
        if result is None:
            current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)
            embed = discord.Embed(
                title="⏰ Hôm nay đã check-in rồi!",
                description=f"Bạn đã hoàn thành check-in hàng ngày rồi!\n\n💎 **Tài sản hiện tại:** {current_cash:,} VND\n🔥 **Chuỗi ngày:** {streak} ngày",
                color=0xffa500
            )
            embed.add_field(
                name="⏰ Lịch trình",
                value="Hãy quay lại vào ngày mai để tiếp tục chuỗi check-in của bạn!",
                inline=False
            )
            await ctx.send(embed=embed)
            return

        # Check for database error
        if result is False:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Đã xảy ra lỗi khi xử lý check-in hàng ngày. Vui lòng thử lại sau ít phút.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Successfully claimed - result is (reward, new_cash, new_streak, old_streak)
        reward, new_cash, new_streak, old_streak = result

        # Create success embed
        embed = discord.Embed(
            title="🎁 Check-in thành công!",
            description=f"**{ctx.author.mention}** đã hoàn thành check-in hàng ngày!",
            color=0x00ff88
        )
        embed.add_field(
            name="💎 Phần thưởng",
            value=f"**+{reward:,} VND**",
            inline=True
        )
        embed.add_field(
            name="🔥 Chuỗi ngày",
            value=f"**{new_streak + 1} ngày**",
            inline=True
        )
        embed.add_field(
            name="💰 Tổng tài sản",
            value=f"**{new_cash:,} VND**",
            inline=True
        )

        if new_streak > old_streak:
            embed.add_field(
                name="🚀 Chuỗi ngày mới!",
                value=f"Chuỗi check-in tăng lên {new_streak + 1} ngày! Phần thưởng ngày mai sẽ cao hơn!",
                inline=False
            )
        elif new_streak == 0 and old_streak > 0:
            embed.add_field(
                name="💔 Chuỗi ngày bị ngắt",
                value="Bạn đã bỏ lỡ một ngày, chuỗi check-in đã được khởi động lại từ ngày 1.",
                inline=False
            )

        embed.set_footer(text="Hãy nhớ check-in hàng ngày để duy trì chuỗi ngày! 🔥")
        await ctx.send(embed=embed)

    @bot.command(name='cashboard')
    async def cash_leaderboard(ctx, page: int = 1):
        """Show cash leaderboard with pagination"""
        guild_id = str(ctx.guild.id)

        try:
            # Try database first, fall back to memory if database unavailable
            connection = bot._get_db_connection()
            users_data = []

            if connection:
                # Use database data
                with connection.cursor() as cursor:
                    cursor.execute(
                        """SELECT user_id, cash, daily_streak 
                           FROM user_cash 
                           WHERE guild_id = %s AND cash > 0 
                           ORDER BY cash DESC""",
                        (guild_id,)
                    )
                    results = cursor.fetchall()
                    users_data = [(user_id, cash, streak) for user_id, cash, streak in results]
                connection.close()
            else:
                # Use in-memory data when database is unavailable
                for key, data in bot.user_cash_memory.items():
                    if key.startswith(f"{guild_id}_") and data.get('cash', 0) > 0:
                        user_id = key.split('_', 1)[1]  # Extract user_id from "guild_id_user_id"
                        cash = data.get('cash', 0)
                        streak = data.get('daily_streak', 0)
                        users_data.append((user_id, cash, streak))

                # Sort by cash (descending)
                users_data.sort(key=lambda x: x[1], reverse=True)

            total_users = len(users_data)

            if total_users == 0:
                embed = discord.Embed(
                    title="📈 Bảng xếp hạng Cash",
                    description="Chưa có ai có tiền trong máy chủ này!\n\nDùng `?daily` để bắt đầu kiếm cash!",
                    color=0x5865f2
                )
                await ctx.send(embed=embed)
                return

            # Calculate pagination
            per_page = 10
            total_pages = (total_users + per_page - 1) // per_page

            if page < 1 or page > total_pages:
                embed = discord.Embed(
                    title="❌ Trang không hợp lệ",
                    description=f"Vui lòng chọn trang từ 1 đến {total_pages}",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return

            # Get data for this page
            start_idx = (page - 1) * per_page
            end_idx = start_idx + per_page
            page_data = users_data[start_idx:end_idx]

            embed = discord.Embed(
                title="🏆 Bảng xếp hạng Cash",
                description=f"💰 **Top người giàu nhất trong máy chủ**\n📄 Trang {page}/{total_pages}",
                color=0xffd700
            )

            for i, (user_id, cash, streak) in enumerate(page_data):
                try:
                    user = await bot.fetch_user(int(user_id))
                    rank = start_idx + i + 1

                    if rank == 1:
                        rank_emoji = "🥇"
                    elif rank == 2:
                        rank_emoji = "🥈" 
                    elif rank == 3:
                        rank_emoji = "🥉"
                    else:
                        rank_emoji = f"{rank}."

                    embed.add_field(
                        name=f"{rank_emoji} {user.display_name}",
                        value=f"💰 **{cash:,} cash**\n🔥 {streak} ngày streak",
                        inline=True
                    )
                except:
                    # Skip if user can't be fetched
                    continue

            if total_pages > 1:
                embed.set_footer(text=f"Dùng ?cashboard <số trang> để xem trang khác • Trang {page}/{total_pages}")
            else:
                embed.set_footer(text="Dùng ?daily để kiếm cash!")

            # Add note about data source
            if not connection:
                embed.add_field(
                    name="ℹ️ Thông tin",
                    value="Dữ liệu từ bộ nhớ tạm (database không khả dụng)",
                    inline=False
                )

            await ctx.send(embed=embed)

        except Exception as e:
            logger.error(f"Error getting cash leaderboard: {e}")
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Có lỗi xảy ra khi lấy bảng xếp hạng. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    # === OVER/UNDER GAME COMMANDS ===
    @bot.command(name='tx')
    async def start_overunder(ctx):
        """Start an Over/Under betting game"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        game_id = f"{guild_id}_{channel_id}_{int(datetime.utcnow().timestamp())}"

        # Check if there's already an active game in this channel
        if guild_id in bot.overunder_games:
            for existing_game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    embed = discord.Embed(
                        title="⚠️ Đã có game đang diễn ra!",
                        description="Kênh này đã có một game Over/Under đang diễn ra. Vui lòng đợi game hiện tại kết thúc.",
                        color=0xffa500
                    )
                    await ctx.send(embed=embed)
                    return

        # Create new game
        end_time = datetime.utcnow() + timedelta(seconds=30)

        if guild_id not in bot.overunder_games:
            bot.overunder_games[guild_id] = {}

        bot.overunder_games[guild_id][game_id] = {
            'channel_id': channel_id,
            'end_time': end_time,
            'bets': [],
            'status': 'active',
            'result': None,
            'end_task': None
        }

        # Store in database
        try:
            connection = bot._get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "INSERT INTO overunder_games (game_id, guild_id, channel_id) VALUES (%s, %s, %s)",
                        (game_id, guild_id, channel_id)
                    )
                    connection.commit()
                connection.close()
        except Exception as e:
            logger.error(f"Error storing game in database: {e}")

        embed = discord.Embed(
            title="🎲 Game Đoán Số Bắt Đầu!",
            description="**Chào mừng bạn tham gia game đoán số hấp dẫn!**\n\nHãy dự đoán kết quả sẽ là Tài (cao) hay Xỉu (thấp)!",
            color=0x00ff88
        )
        embed.add_field(
            name="⏱️ Thời gian cược",
            value="**30 giây** để đặt cược",
            inline=True
        )
        embed.add_field(
            name="🎯 Hướng dẫn",
            value="Gõ `?cuoc <tai/xiu> <số tiền>`",
            inline=True
        )
        embed.add_field(
            name="💸 Tiền thưởng",
            value="**Nhân đôi** số tiền cược khi thắng!",
            inline=True
        )
        embed.add_field(
            name="📝 Ví dụ thực tế",
            value="`?cuoc tai 1000` - Đặt cược 1000 VND vào Tài\n`?cuoc xiu 500` - Đặt cược 500 VND vào Xỉu",
            inline=False
        )
        embed.set_footer(text=f"Game ID: {game_id} • Kết thúc lúc {end_time.strftime('%H:%M:%S')}")

        await ctx.send(embed=embed)

        # Schedule game end
        game_task = asyncio.create_task(bot._end_overunder_game(guild_id, game_id))
        bot.overunder_games[guild_id][game_id]['end_task'] = game_task

    @bot.command(name='cuoc')
    async def place_bet(ctx, side=None, amount=None):
        """Place a bet in the Tai/Xiu game"""
        if not side or not amount:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?cuoc <tai/xiu> <số tiền>`\n\n**Ví dụ:**\n`?cuoc tai 1000` - Cược 1,000 cash\n`?cuoc xiu 5k` - Cược 5,000 cash\n`?cuoc tai 1.5m` - Cược 1,500,000 cash\n`?cuoc xiu 2b` - Cược 2,000,000,000 cash\n`?cuoc tai 5t` - Cược 5,000,000,000,000 cash\n`?cuoc xiu 1qa` - Cược 1,000,000,000,000,000 cash\n`?cuoc tai 2qi` - Cược 2,000,000,000,000,000,000 cash\n`?cuoc xiu 1sx` - Cược 1,000,000,000,000,000,000,000 cash\n`?cuoc tai all` - Cược tất cả tiền",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        user_id = str(ctx.author.id)

        # Validate side
        side = side.lower()
        if side not in ['tai', 'xiu']:
            embed = discord.Embed(
                title="❌ Lựa chọn không hợp lệ!",
                description="Bạn chỉ có thể chọn **tai** hoặc **xiu**",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Validate amount with support for k/m/b/t/qa/qi/sx suffixes and 'all'
        def parse_amount(amount_str):
            """Parse amount string with k/m/b/t/qa/qi/sx suffixes and 'all' for all available money"""
            amount_str = amount_str.lower().strip()

            # Handle 'all' - return special value that we'll replace with actual cash
            if amount_str == 'all':
                return -1  # Special value to indicate "all money"

            multiplier = 1

            if amount_str.endswith('sx'):
                multiplier = 1_000_000_000_000_000_000_000  # Sextillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qi'):
                multiplier = 1_000_000_000_000_000_000  # Quintillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qa'):
                multiplier = 1_000_000_000_000_000  # Quadrillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('t'):
                multiplier = 1_000_000_000_000  # Trillion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('b'):
                multiplier = 1_000_000_000  # Billion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('m'):
                multiplier = 1_000_000  # Million
                amount_str = amount_str[:-1]
            elif amount_str.endswith('k'):
                multiplier = 1_000  # Thousand
                amount_str = amount_str[:-1]

            try:
                base_amount = float(amount_str)
                if base_amount <= 0:
                    raise ValueError()
                return int(base_amount * multiplier)
            except (ValueError, OverflowError):
                raise ValueError()

        try:
            bet_amount = parse_amount(amount)
        except ValueError:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ!",
                description="Vui lòng nhập số tiền hợp lệ.\n\n**Ví dụ:** `1000`, `5k`, `1.5m`, `2b`, `5t`, `1qa`, `2qi`, `1sx`, `all`",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Handle 'all' - get user's current cash and bet all of it
        if bet_amount == -1:
            current_cash, _, _ = bot._get_user_cash(guild_id, user_id)
            if current_cash <= 0:
                embed = discord.Embed(
                    title="💸 Tài sản không đủ!",
                    description="Bạn không có đủ tiền để đặt cược.\n\nSử dụng `?daily` để check-in và nhận thưởng!",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return
            bet_amount = current_cash

        # Check if there's an active game in this channel
        active_game = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game = (game_id, game_data)
                    break

        if not active_game:
            embed = discord.Embed(
                title="❌ Không có game nào đang diễn ra!",
                description="Không có game Tài Xỉu nào đang diễn ra trong kênh này. Dùng `?tx` để bắt đầu game mới.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        game_id, game_data = active_game

        # Check if game has ended
        if datetime.utcnow() >= game_data['end_time']:
            embed = discord.Embed(
                title="⏰ Vòng cược đã kết thúc!",
                description="Hết thời gian đặt cược rồi. Đợi kết quả hoặc tạo game mới.",
                color=0xffa500
            )
            await ctx.send(embed=embed)
            return

        # Check user's cash
        current_cash, _, _ = bot._get_user_cash(guild_id, user_id)
        if current_cash < bet_amount:
            embed = discord.Embed(
                title="💸 Tài sản không đủ!",
                description=f"Tài sản của bạn: **{current_cash:,} VND**\nSố tiền muốn cược: **{bet_amount:,} VND**\n\nSử dụng `?daily` để check-in và nhận thưởng!",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Check if user already has a bet in this game
        for bet in game_data['bets']:
            if bet['user_id'] == user_id:
                embed = discord.Embed(
                    title="⚠️ Bạn đã tham gia rồi!",
                    description=f"Bạn đã đặt cược **{bet['amount']:,} VND** vào **{bet['side'].upper()}** cho game này rồi.",
                    color=0xffa500
                )
                await ctx.send(embed=embed)
                return

        # Deduct cash from user
        success = bot._update_user_cash(guild_id, user_id, -bet_amount, None, None)

        if not success:
            embed = discord.Embed(
                title="❌ Xảy ra lỗi!",
                description="Không thể xử lý giao dịch cược của bạn. Vui lòng thử lại sau ít giây.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Calculate remaining cash
        remaining_cash = current_cash - bet_amount

        # Add bet to game
        bet_data = {
            'user_id': user_id,
            'username': ctx.author.display_name,
            'side': side,
            'amount': bet_amount
        }
        game_data['bets'].append(bet_data)

        # Note: Bets are stored in memory during the game
        # Final results are saved to database when game ends

        # Beautiful success embed
        embed = discord.Embed(
            title="🎯 Đặt Cược Thành Công!",
            description=f"🎲 **{ctx.author.display_name}** đã tham gia game Tài Xỉu!",
            color=0x00ff88
        )
        embed.add_field(
            name="🎰 Lựa chọn của bạn",
            value=f"**{'🔺 TÀI' if side == 'tai' else '🔻 XỈU'}**",
            inline=True
        )
        embed.add_field(
            name="💰 Số tiền đã cược",
            value=f"**{bet_amount:,}** cash",
            inline=True
        )
        embed.add_field(
            name="💳 Số dư hiện tại",
            value=f"**{remaining_cash:,}** cash",
            inline=True
        )
        embed.add_field(
            name="🏆 Tiền thưởng nếu thắng",
            value=f"**{bet_amount * 2:,}** cash",
            inline=True
        )
        embed.add_field(
            name="👥 Tổng người chơi",
            value=f"**{len(game_data['bets'])}** người",
            inline=True
        )

        time_left = game_data['end_time'] - datetime.utcnow()
        minutes, seconds = divmod(int(time_left.total_seconds()), 60)
        embed.set_footer(text=f"Thời gian còn lại: {minutes}:{seconds:02d} • Chúc may mắn! 🍀")

        await ctx.send(embed=embed)


    @bot.command(name='txshow')
    async def show_overunder_result(ctx):
        """Start continuous auto-cycling: end current round, show winner, auto-start new rounds until gamestop"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        channel_key = f"{guild_id}_{channel_id}"

        # Find active game in this channel
        active_game_id = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game_id = game_id
                    break

        if not active_game_id:
            embed = discord.Embed(
                title="❌ Không có game Tài Xỉu",
                description="Hiện tại không có game Tài Xỉu nào đang chạy trong kênh này.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Enable auto-cycle for this channel
        bot.overunder_autocycle[channel_key] = True

        # End current game immediately and show results
        embed = discord.Embed(
            title="🔄 Bắt đầu chế độ tự động!",
            description="Game hiện tại sẽ kết thúc và tự động bắt đầu game mới liên tục!\n\nDùng `?gamestop` để dừng.",
            color=0x00ff88
        )
        await ctx.send(embed=embed)

        # End game immediately - this will trigger auto-cycle
        await bot._end_overunder_game(guild_id, active_game_id, instant_stop=True)

    @bot.command(name='gamestop')
    async def stop_overunder(ctx):
        """Stop the current Tai/Xiu game instantly and show results"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)

        # Find active game in this channel
        active_game_id = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game_id = game_id
                    break

        if not active_game_id:
            embed = discord.Embed(
                title="❌ Không có game Tài Xỉu",
                description="Hiện tại không có game Tài Xỉu nào đang chạy trong kênh này.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Stop auto-cycle if active
        channel_key = f"{guild_id}_{channel_id}"
        if channel_key in bot.overunder_autocycle:
            del bot.overunder_autocycle[channel_key]
            embed = discord.Embed(
                title="⏹️ Dừng chế độ tự động",
                description="Đã tắt chế độ tự động và dừng game Tài Xỉu! Đang công bố kết quả cuối cùng...",
                color=0xffa500
            )
        else:
            embed = discord.Embed(
                title="⏹️ Dừng game Tài Xỉu",
                description="Game Tài Xỉu đã được dừng! Đang công bố kết quả...",
                color=0xffa500
            )
        await ctx.send(embed=embed)

        # End game immediately
        await bot._end_overunder_game(guild_id, active_game_id, instant_stop=True)

    @bot.command(name='reset_questions')
    @commands.has_permissions(administrator=True)
    async def reset_questions(ctx):
        """Reset question history for the server (Admin only)"""
        guild_id = str(ctx.guild.id)
        bot._reset_question_history(guild_id)

        embed = discord.Embed(
            title="🔄 Lịch sử câu hỏi đã được reset",
            description="Tất cả câu hỏi có thể được hỏi lại từ đầu.\n\nNgười chơi sẽ gặp các câu hỏi đã hỏi trước đó trong phiên chơi mới.",
            color=0x00ff88
        )
        await ctx.send(embed=embed)

    @bot.command(name='moneyhack')
    @commands.has_permissions(administrator=True)
    async def moneyhack(ctx, amount_str: str, user: Optional[discord.Member] = None):
        """Give money to a user (Admin only) - supports up to 50 digits"""
        if user is None:
            user = ctx.author

        guild_id = str(ctx.guild.id)
        user_id = str(user.id)

        try:
            # Clean the string from commas or spaces
            amount_str = amount_str.replace(',', '').replace(' ', '')
            if len(amount_str) > 50:
                embed = discord.Embed(
                    title="❌ Giới hạn vượt mức",
                    description="Số tiền quá lớn! Tối đa là 50 chữ số.",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return

            amount = int(amount_str)
            if amount <= 0:
                raise ValueError()
        except ValueError:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ",
                description="Vui lòng nhập một số nguyên dương hợp lệ (tối đa 50 chữ số).",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get current cash
        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)
        new_cash = current_cash + amount

        # Update user's cash
        success = bot._update_user_cash(guild_id, user_id, new_cash, last_daily, streak)

        if success:
            embed = discord.Embed(
                title="💰 Money Hack Thành Công!",
                description=f"**Admin {ctx.author.mention}** đã tặng tiền cho **{user.mention}**",
                color=0x00ff88
            )
            # Use custom formatting for very large numbers
            def format_large(n):
                return f"{n:,}" if n < 10**15 else str(n)

            embed.add_field(
                name="💵 Số tiền tặng",
                value=f"**+{format_large(amount)} cash**",
                inline=True
            )
            embed.add_field(
                name="💳 Số dư mới",
                value=f"**{format_large(new_cash)} cash**",
                inline=True
            )
            embed.set_footer(text="Chỉ Admin mới có thể sử dụng lệnh này!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể cập nhật số dư. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='give')
    async def give_money(ctx, user: discord.Member = None, amount: str = None):
        """Give money to another user"""
        if user is None or amount is None:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?give <@user> <số tiền>`\n\n**Ví dụ:**\n`?give @user 1000` - Tặng 1,000 cash\n`?give @user 5k` - Tặng 5,000 cash\n`?give @user 1.5m` - Tặng 1,500,000 cash\n`?give @user 2b` - Tặng 2,000,000,000 cash\n`?give @user 5t` - Tặng 5,000,000,000,000 cash\n`?give @user all` - Tặng tất cả tiền của bạn",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        giver_id = str(ctx.author.id)
        receiver_id = str(user.id)

        # Don't let users give money to themselves
        if giver_id == receiver_id:
            embed = discord.Embed(
                title="❌ Không thể tự tặng tiền cho mình!",
                description="Bạn không thể tặng tiền cho chính mình.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Parse amount with support for k/m/b/t/qa/qi/sx suffixes and 'all'
        def parse_amount(amount_str):
            """Parse amount string with k/m/b/t/qa/qi/sx suffixes and 'all' for all available money"""
            amount_str = amount_str.lower().strip()

            # Handle 'all' - return special value that we'll replace with actual cash
            if amount_str == 'all':
                return -1  # Special value to indicate "all money"

            multiplier = 1

            if amount_str.endswith('sx'):
                multiplier = 1_000_000_000_000_000_000_000  # Sextillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qi'):
                multiplier = 1_000_000_000_000_000_000  # Quintillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('qa'):
                multiplier = 1_000_000_000_000_000  # Quadrillion
                amount_str = amount_str[:-2]
            elif amount_str.endswith('t'):
                multiplier = 1_000_000_000_000  # Trillion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('b'):
                multiplier = 1_000_000_000  # Billion
                amount_str = amount_str[:-1]
            elif amount_str.endswith('m'):
                multiplier = 1_000_000  # Million
                amount_str = amount_str[:-1]
            elif amount_str.endswith('k'):
                multiplier = 1_000  # Thousand
                amount_str = amount_str[:-1]

            try:
                base_amount = float(amount_str)
                if base_amount <= 0:
                    raise ValueError()
                return int(base_amount * multiplier)
            except (ValueError, OverflowError):
                raise ValueError()

        try:
            give_amount = parse_amount(amount)
        except ValueError:
            embed = discord.Embed(
                title="❌ Số tiền không hợp lệ!",
                description="Vui lòng nhập số tiền hợp lệ.\n\n**Ví dụ:** `1000`, `5k`, `1.5m`, `2b`, `5t`, `1qa`, `2qi`, `1sx`, `all`",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get giver's current cash
        giver_cash, giver_daily, giver_streak = bot._get_user_cash(guild_id, giver_id)

        # Handle 'all' - give all of giver's money
        if give_amount == -1:
            if giver_cash <= 0:
                embed = discord.Embed(
                    title="💸 Không có tiền để tặng!",
                    description="Bạn không có tiền để tặng cho ai.\n\nDùng `?daily` để nhận thưởng hàng ngày!",
                    color=0xff4444
                )
                await ctx.send(embed=embed)
                return
            give_amount = giver_cash

        # Check if giver has enough money
        if giver_cash < give_amount:
            embed = discord.Embed(
                title="💸 Không đủ tiền!",
                description=f"Bạn chỉ có **{giver_cash:,} cash** nhưng muốn tặng **{give_amount:,} cash**.\n\nDùng `?money` để kiểm tra số dư.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Get receiver's current cash
        receiver_cash, receiver_daily, receiver_streak = bot._get_user_cash(guild_id, receiver_id)

        # Update both users' cash
        new_giver_cash = giver_cash - give_amount
        new_receiver_cash = receiver_cash + give_amount

        # Update giver's cash (subtract)
        success1 = bot._update_user_cash(guild_id, giver_id, new_giver_cash, giver_daily, giver_streak)
        # Update receiver's cash (add)
        success2 = bot._update_user_cash(guild_id, receiver_id, new_receiver_cash, receiver_daily, receiver_streak)

        if success1 and success2:
            embed = discord.Embed(
                title="💝 Chuyển tiền thành công!",
                description=f"**{ctx.author.mention}** đã tặng tiền cho **{user.mention}**",
                color=0x00ff88
            )
            embed.add_field(
                name="💰 Số tiền tặng",
                value=f"**{give_amount:,} cash**",
                inline=True
            )
            embed.add_field(
                name="👤 Người tặng",
                value=f"{ctx.author.mention}\n💳 Còn lại: **{new_giver_cash:,} cash**",
                inline=True
            )
            embed.add_field(
                name="🎁 Người nhận",
                value=f"{user.mention}\n💳 Tổng cộng: **{new_receiver_cash:,} cash**",
                inline=True
            )
            embed.set_footer(text="Cảm ơn bạn đã chia sẻ!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể thực hiện giao dịch. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='clear')
    @commands.has_permissions(administrator=True)
    async def clear_money(ctx, user: discord.Member = None):
        """Reset a user's money to 0 (Admin only)"""
        if user is None:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?clear <@user>`\n\n**Ví dụ:**\n`?clear @user` - Reset tiền của user về 0",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        user_id = str(user.id)

        # Get user's current cash
        current_cash, last_daily, streak = bot._get_user_cash(guild_id, user_id)

        # Reset user's cash to 0
        success = bot._update_user_cash(guild_id, user_id, 0, last_daily, streak)

        if success:
            embed = discord.Embed(
                title="🗑️ Reset tiền thành công!",
                description=f"**Admin {ctx.author.mention}** đã reset tiền của **{user.mention}**",
                color=0x00ff88
            )
            embed.add_field(
                name="💰 Tiền trước đó",
                value=f"**{current_cash:,} cash**",
                inline=True
            )
            embed.add_field(
                name="💳 Tiền hiện tại",
                value="**0 cash**",
                inline=True
            )
            embed.set_footer(text="Chỉ Admin mới có thể sử dụng lệnh này!")
            await ctx.send(embed=embed)
        else:
            embed = discord.Embed(
                title="❌ Lỗi hệ thống",
                description="Không thể reset tiền của người dùng. Vui lòng thử lại sau.",
                color=0xff4444
            )
            await ctx.send(embed=embed)

    @bot.command(name='win')
    @commands.has_permissions(administrator=True)
    async def set_winner(ctx, result: str = None):
        """Manually set the winner of the current game (Admin only)"""
        if not result:
            embed = discord.Embed(
                title="❌ Sai cú pháp!",
                description="Cách sử dụng: `?win <tai/xiu>`\n\n**Ví dụ:**\n`?win tai` - Đặt kết quả là Tài\n`?win xiu` - Đặt kết quả là Xỉu",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)

        # Validate result
        result = result.lower()
        if result not in ['tai', 'xiu']:
            embed = discord.Embed(
                title="❌ Kết quả không hợp lệ!",
                description="Bạn chỉ có thể chọn **tai** hoặc **xiu**",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        # Check if there's an active game in this channel
        active_game = None
        if guild_id in bot.overunder_games:
            for game_id, game_data in bot.overunder_games[guild_id].items():
                if game_data['channel_id'] == channel_id and game_data['status'] == 'active':
                    active_game = (game_id, game_data)
                    break

        if not active_game:
            embed = discord.Embed(
                title="❌ Không có game nào đang diễn ra!",
                description="Không có game Tài Xỉu nào đang diễn ra trong kênh này. Dùng `?tx` để bắt đầu game mới.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return

        game_id, game_data = active_game

        # Set the result manually
        game_data['result'] = result
        game_data['status'] = 'ended'

        # Update database
        try:
            connection = bot._get_db_connection()
            if connection:
                with connection.cursor() as cursor:
                    cursor.execute(
                        "UPDATE overunder_games SET result = %s, status = 'ended' WHERE game_id = %s",
                        (result, game_id)
                    )
                    connection.commit()
                connection.close()
        except Exception as e:
            logger.error(f"Error updating game result: {e}")

        # Show admin action first
        embed = discord.Embed(
            title="⚙️ Admin đã đặt kết quả!",
            description=f"**Admin {ctx.author.mention}** đã đặt kết quả game là **{result.upper()}**",
            color=0xffa500
        )
        embed.set_footer(text="Game sẽ kết thúc ngay lập tức...")
        await ctx.send(embed=embed)

        # Process the game ending with the set result
        winners = []
        losers = []
        total_winners = 0
        total_losers = 0
        total_winnings = 0

        for bet in game_data['bets']:
            if bet['side'] == result:
                winners.append(bet)
                total_winners += 1
                total_winnings += bet['amount']
            else:
                losers.append(bet)
                total_losers += 1

        # Distribute winnings (2x payout)
        for bet in winners:
            user_id = bet['user_id']
            winnings = bet['amount'] * 2  # 2x payout for winning bets
            bot._update_user_cash(guild_id, user_id, winnings)

        # Create result embed
        result_embed = discord.Embed(
            title="🎲 Kết quả game Tài Xỉu!",
            description=f"**Kết quả:** {result.upper()} {'🔺' if result == 'tai' else '🔻'}\n\n*Kết quả được đặt bởi Admin*",
            color=0x00ff88 if result == 'tai' else 0xff6b6b
        )

        result_embed.add_field(
            name="🏆 Người thắng",
            value=f"**{total_winners}** người thắng\n💰 Tổng thưởng: **{total_winnings * 2:,} cash**",
            inline=True
        )

        result_embed.add_field(
            name="💸 Người thua",
            value=f"**{total_losers}** người thua\n💔 Mất: **{sum(bet['amount'] for bet in losers):,} cash**",
            inline=True
        )

        result_embed.add_field(
            name="💡 Lưu ý",
            value="Người thắng nhận lại 2x số tiền đã cược!\nDùng `?tx` để bắt đầu game mới.",
            inline=False
        )

        await ctx.send(embed=result_embed)

        # Clean up the game
        if guild_id in bot.overunder_games and game_id in bot.overunder_games[guild_id]:
            del bot.overunder_games[guild_id][game_id]
            if not bot.overunder_games[guild_id]:
                del bot.overunder_games[guild_id]

    # Error handling
    @bot.event
    async def on_command_error(ctx, error):
        """Handle command errors"""
        if isinstance(error, commands.MissingPermissions):
            embed = discord.Embed(
                title="🚫 Access Denied",
                description="You don't have permission to use this command.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
        elif isinstance(error, commands.BotMissingPermissions):
            embed = discord.Embed(
                title="⚠️ Missing Permissions",
                description="Tôi vốn deck có quyền để thực thi lệnh, vui lòng cấp quyền!",
                color=0xffa500
            )
            await ctx.send(embed=embed)
        elif isinstance(error, commands.CommandNotFound):
            return  # Ignore command not found errors
        else:
            logger.error(f"Command error: {error}")
            embed = discord.Embed(
                title="💥 Command Error",
                description="An unexpected error occurred while executing the command.",
                color=0xff4444
            )
            await ctx.send(embed=embed)
