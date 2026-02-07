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

    def _save_backup_data(self):
        """Save current user cash data to backup file"""
        try:
            if not self.user_cash_memory:
                return

            save_memory = {}
            for key, data in self.user_cash_memory.items():
                processed_data = data.copy()
                if 'last_daily' in processed_data and processed_data['last_daily']:
                    if hasattr(processed_data['last_daily'], 'isoformat'):
                        processed_data['last_daily'] = processed_data['last_daily'].isoformat()
                save_memory[key] = processed_data

            backup_data = {
                'user_cash_memory': save_memory,
                'last_backup': datetime.utcnow().isoformat()
            }

            temp_file = f"{self.backup_file_path}.tmp"
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)

            os.replace(temp_file, self.backup_file_path)
            logger.debug(f"Saved backup data for {len(save_memory)} users")
        except Exception as e:
            logger.error(f"Error saving backup data: {e}")

    async def _backup_data_loop(self):
        """Background task that saves data every 5 seconds"""
        await asyncio.sleep(10)
        while True:
            try:
                await asyncio.sleep(5)
                self._save_backup_data()
            except Exception as e:
                logger.error(f"Error in backup loop: {e}")
                await asyncio.sleep(30)

    def _get_user_cash(self, guild_id, user_id):
        """Get user's cash amount and daily streak info"""
        key = f"{guild_id}_{user_id}"
        if key in self.user_cash_memory:
            data = self.user_cash_memory[key]
            return data.get('cash', 1000), data.get('last_daily'), data.get('daily_streak', 0)
        else:
            return 1000, None, 0

    def _update_user_cash(self, guild_id, user_id, cash_amount, last_daily=None, daily_streak=None):
        """Update user's cash amount and daily streak"""
        key = f"{guild_id}_{user_id}"
        if key not in self.user_cash_memory:
            self.user_cash_memory[key] = {'cash': 1000, 'last_daily': None, 'daily_streak': 0}

        if last_daily is not None and daily_streak is not None:
            self.user_cash_memory[key].update({
                'cash': cash_amount,
                'last_daily': last_daily,
                'daily_streak': daily_streak
            })
        else:
            self.user_cash_memory[key]['cash'] += cash_amount

        self._save_backup_data()
        return True

    def _calculate_daily_reward(self, streak):
        reward_table = {
            1: 1000, 2: 2000, 3: 5000, 4: 10000, 5: 20000,
            6: 50000, 7: 100000, 8: 200000, 9: 500000, 10: 1000000,
            11: 1500000, 12: 2000000, 13: 3000000, 14: 5000000, 15: 7000000,
            16: 10000000, 17: 15000000, 18: 20000000, 19: 25000000, 20: 30000000
        }
        if streak in reward_table:
            return reward_table[streak]
        if streak > 20:
            return 30000000 + (5000000 * (streak - 20))
        return 1000

    async def _claim_daily_reward(self, guild_id, user_id, today):
        if isinstance(today, datetime):
            today = today.date()
        
        key = f"{guild_id}_{user_id}"
        if key not in self._daily_locks:
            self._daily_locks[key] = asyncio.Lock()
        
        async with self._daily_locks[key]:
            if key not in self.user_cash_memory:
                self.user_cash_memory[key] = {'cash': 1000, 'last_daily': None, 'daily_streak': 0}
            
            current_data = self.user_cash_memory[key]
            last_daily = current_data.get('last_daily')
            
            if isinstance(last_daily, datetime):
                last_daily = last_daily.date()
            elif isinstance(last_daily, str):
                try:
                    last_daily = datetime.strptime(last_daily, '%Y-%m-%d').date()
                except (ValueError, TypeError):
                    last_daily = None
            
            if last_daily == today:
                return None
            
            new_streak = 1
            if last_daily:
                if today.month == last_daily.month and today.year == last_daily.year:
                    yesterday = today - timedelta(days=1)
                    if last_daily == yesterday:
                        new_streak = current_data.get('daily_streak', 0) + 1
            
            reward = self._calculate_daily_reward(new_streak)
            current_cash = current_data.get('cash', 1000)
            new_total = current_cash + reward
            
            self._update_user_cash(guild_id, user_id, new_total, today, new_streak)
            return reward, new_total, new_streak, current_data.get('daily_streak', 0)

    async def _end_overunder_game(self, guild_id, game_id, instant_stop=False):
        """End the game, roll dice, and distribute rewards"""
        if not instant_stop:
            await asyncio.sleep(150)
        
        if guild_id not in self.overunder_games or game_id not in self.overunder_games[guild_id]:
            return

        game_data = self.overunder_games[guild_id][game_id]
        if game_data['status'] != 'active':
            return

        game_data['status'] = 'ended'
        channel = self.get_channel(int(game_data['channel_id']))
        if not channel:
            return

        # Roll dice
        dice = [random.randint(1, 6) for _ in range(3)]
        total = sum(dice)
        
        if hasattr(self, 'manual_win_result') and self.manual_win_result:
            result = self.manual_win_result
            if result == 'tai' and total <= 10:
                dice = [random.randint(4, 6) for _ in range(3)]
                total = sum(dice)
            elif result == 'xiu' and total >= 11:
                dice = [random.randint(1, 3) for _ in range(3)]
                total = sum(dice)
            self.manual_win_result = None
        else:
            result = 'tai' if total >= 11 else 'xiu'

        # Process bets
        winners = [b for b in game_data['bets'] if b['side'] == result]
        losers = [b for b in game_data['bets'] if b['side'] != result]
        
        for bet in winners:
            self._update_user_cash(guild_id, bet['user_id'], bet['amount'] * 2)

        # Send results
        dice_emojis = {1: "⚀", 2: "⚁", 3: "⚂", 4: "⚃", 5: "⚄", 6: "⚅"}
        dice_str = " ".join([dice_emojis[d] for d in dice])
        
        embed = discord.Embed(
            title=f"🎲 KẾT QUẢ TÀI XỈU: {result.upper()}",
            description=f"**Xúc xắc:** {dice_str}\n**Tổng điểm:** {total}",
            color=0x00ff88 if result == 'tai' else 0xff4444
        )
        
        winner_mentions = ", ".join([f"**{b['username']}**" for b in winners]) if winners else "Không có"
        embed.add_field(name=f"🎉 Người thắng ({result.upper()})", value=winner_mentions, inline=False)
        
        total_won = sum(b['amount'] for b in winners)
        embed.add_field(name="💰 Tổng tiền thắng", value=f"{total_won:,} VND", inline=True)
        embed.add_field(name="👥 Tổng người cược", value=str(len(game_data['bets'])), inline=True)
        
        await channel.send(embed=embed)
        
        # Auto-cycle check
        key = f"{guild_id}_{game_data['channel_id']}"
        if key in self.overunder_autocycle:
            await asyncio.sleep(5)
            ctx = await self.get_context(await channel.fetch_message(channel.last_message_id))
            await self.get_command('tx').callback(self.get_cog('TaiXiu'), ctx)

bot = TaiXiuBot()

class TaiXiu(commands.Cog):
    def __init__(self, bot):
        self.bot = bot

    def parse_amount(self, amount_str):
        amount_str = amount_str.lower().strip()
        if amount_str == 'all':
            return -1
        
        multiplier = 1
        suffixes = {
            'sx': 10**21, 'qi': 10**18, 'qa': 10**15, 't': 10**12,
            'b': 10**9, 'm': 10**6, 'k': 10**3
        }
        
        for suffix, mult in suffixes.items():
            if amount_str.endswith(suffix):
                multiplier = mult
                amount_str = amount_str[:-len(suffix)]
                break
                
        try:
            base_amount = float(amount_str)
            if base_amount <= 0:
                raise ValueError()
            return int(base_amount * multiplier)
        except (ValueError, OverflowError):
            raise ValueError()

    @commands.command(name='money')
    async def money_command(self, ctx, user: discord.Member = None):
        user = user or ctx.author
        cash, _, streak = self.bot._get_user_cash(str(ctx.guild.id), str(user.id))
        embed = discord.Embed(title="💰 Tài Khoản Cá Nhân", color=0x00ff88)
        embed.add_field(name="💳 Số dư", value=f"**{cash:,} VND**", inline=False)
        embed.add_field(name="🔥 Chuỗi", value=f"**{streak} ngày**", inline=False)
        await ctx.send(embed=embed)

    @commands.command(name='daily')
    async def daily_command(self, ctx):
        result = await self.bot._claim_daily_reward(str(ctx.guild.id), str(ctx.author.id), datetime.utcnow().date())
        if result is None:
            cash, _, streak = self.bot._get_user_cash(str(ctx.guild.id), str(ctx.author.id))
            await ctx.send(f"⏰ Bạn đã nhận quà rồi! Số dư: {cash:,} VND")
            return
        reward, total, streak, _ = result
        await ctx.send(f"🎁 Chúc mừng! Bạn nhận được **{reward:,} VND**. Số dư mới: **{total:,} VND** (Chuỗi {streak} ngày)")

    @commands.command(name='cashboard')
    async def show_cashboard(self, ctx):
        guild_id = str(ctx.guild.id)
        users = []
        for key, data in self.bot.user_cash_memory.items():
            if key.startswith(f"{guild_id}_"):
                users.append((key.split('_')[1], data.get('cash', 0)))
        
        if not users:
            await ctx.send("Chưa có ai!")
            return
            
        sorted_users = sorted(users, key=lambda x: x[1], reverse=True)
        embed = discord.Embed(title="🏆 Bảng Xếp Hạng", color=0xffd700)
        for i, (u_id, cash) in enumerate(sorted_users[:10]):
            try:
                user = await self.bot.fetch_user(int(u_id))
                embed.add_field(name=f"{i+1}. {user.display_name}", value=f"{cash:,} VND", inline=True)
            except: continue
        await ctx.send(embed=embed)

    @commands.command(name='tx')
    async def start_overunder(self, ctx):
        guild_id, channel_id = str(ctx.guild.id), str(ctx.channel.id)
        if guild_id not in self.bot.overunder_games: self.bot.overunder_games[guild_id] = {}
        for g_data in self.bot.overunder_games[guild_id].values():
            if g_data['channel_id'] == channel_id and g_data['status'] == 'active':
                await ctx.send("Đang có game!")
                return

        game_id = f"tx_{int(time.time())}"
        self.bot.overunder_games[guild_id][game_id] = {
            'channel_id': channel_id, 'status': 'active', 'bets': [], 'created_at': datetime.utcnow()
        }
        await ctx.send("🎲 TRÒ CHƠI TÀI XỈU MỚI! Dùng `?cuoc <tai/xiu> <tiền>`")
        asyncio.create_task(self.bot._end_overunder_game(guild_id, game_id))

    @commands.command(name='cuoc')
    async def place_bet(self, ctx, side: str, amount: str):
        side = side.lower()
        if side not in ['tai', 'xiu']: return
        guild_id, user_id, channel_id = str(ctx.guild.id), str(ctx.author.id), str(ctx.channel.id)
        try: bet_amount = self.parse_amount(amount)
        except: return
        
        cash, _, _ = self.bot._get_user_cash(guild_id, user_id)
        if bet_amount == -1: bet_amount = cash
        if cash < bet_amount:
            await ctx.send("Không đủ tiền!")
            return

        active_game = None
        if guild_id in self.bot.overunder_games:
            for g_id, g_data in self.bot.overunder_games[guild_id].items():
                if g_data['channel_id'] == channel_id and g_data['status'] == 'active':
                    active_game = (g_id, g_data)
                    break
        
        if not active_game: return
        self.bot._update_user_cash(guild_id, user_id, -bet_amount)
        active_game[1]['bets'].append({'user_id': user_id, 'username': ctx.author.display_name, 'side': side, 'amount': bet_amount})
        await ctx.send(f"✅ Đã cược **{bet_amount:,} VND** vào **{side.upper()}**!")

    @commands.command(name='txshow')
    async def txshow(self, ctx):
        self.bot.overunder_autocycle[f"{ctx.guild.id}_{ctx.channel.id}"] = True
        await ctx.send("🔄 Tự động bật!")

    @commands.command(name='gamestop')
    async def gamestop(self, ctx):
        key = f"{ctx.guild.id}_{ctx.channel.id}"
        if key in self.bot.overunder_autocycle: del self.bot.overunder_autocycle[key]
        await ctx.send("⏹️ Đã dừng!")

async def main():
    async with bot:
        await bot.add_cog(TaiXiu(bot))
        token = os.getenv('DISCORD_BOT_TOKEN')
        if not token:
            logger.error("DISCORD_BOT_TOKEN not set!")
            return
        
        restart_count = 0
        while restart_count < 100:
            try:
                logger.info(f"Starting Tai Xiu bot (attempt {restart_count + 1})")
                await bot.start(token)
            except Exception as e:
                restart_count += 1
                logger.error(f"Bot crashed: {e}")
                await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
