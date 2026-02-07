import discord
from discord.ext import commands
import random
import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from typing import Optional

logger = logging.getLogger(__name__)

class TaiXiu(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        # Ensure the bot has the necessary storage attributes if they don't exist
        if not hasattr(self.bot, 'overunder_games'):
            self.bot.overunder_games = {}
        if not hasattr(self.bot, 'overunder_autocycle'):
            self.bot.overunder_autocycle = {}
        if not hasattr(self.bot, 'user_cash_memory'):
            self.bot.user_cash_memory = {}
        if not hasattr(self.bot, '_daily_locks'):
            self.bot._daily_locks = {}

    def parse_amount(self, amount_str):
        """Parse amount string with k/m/b/t/qa/qi/sx suffixes and 'all'"""
        amount_str = amount_str.lower().strip()
        if amount_str == 'all':
            return -1
        
        multiplier = 1
        suffixes = {
            'sx': 1_000_000_000_000_000_000_000,
            'qi': 1_000_000_000_000_000_000,
            'qa': 1_000_000_000_000_000,
            't': 1_000_000_000_000,
            'b': 1_000_000_000,
            'm': 1_000_000,
            'k': 1_000
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
        """Check your cash balance"""
        if user is None:
            user = ctx.author
            
        guild_id = str(ctx.guild.id)
        user_id = str(user.id)
        
        cash, _, streak = self.bot._get_user_cash(guild_id, user_id)
        
        embed = discord.Embed(
            title="💰 Tài Khoản Cá Nhân",
            description=f"Thông tin tài chính của **{user.display_name}**",
            color=0x00ff88
        )
        embed.add_field(name="💳 Số dư hiện tại", value=f"**{cash:,} VND**", inline=False)
        embed.add_field(name="🔥 Chuỗi đăng nhập", value=f"**{streak} ngày**", inline=False)
        embed.set_thumbnail(url=user.display_avatar.url if user.display_avatar else None)
        
        await ctx.send(embed=embed)

    @commands.command(name='daily')
    async def daily_command(self, ctx):
        """Claim daily reward"""
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)
        today = datetime.utcnow().date()
        
        result = await self.bot._claim_daily_reward(guild_id, user_id, today)
        
        if result is None:
            current_cash, _, streak = self.bot._get_user_cash(guild_id, user_id)
            embed = discord.Embed(
                title="⏰ Bạn đã nhận quà rồi!",
                description=f"Hôm nay bạn đã nhận phần thưởng rồi. Hãy quay lại vào ngày mai!\n\n💎 **Tài sản hiện tại:** {current_cash:,} VND\n🔥 **Chuỗi ngày:** {streak} ngày",
                color=0xff4444
            )
            await ctx.send(embed=embed)
            return
            
        reward, total_cash, streak, old_streak = result
        
        embed = discord.Embed(
            title="🎁 Phần Thưởng Hàng Ngày",
            description=f"Chúc mừng **{ctx.author.display_name}**!",
            color=0x00ff88
        )
        embed.add_field(name="💰 Tiền thưởng", value=f"**+{reward:,} VND**", inline=True)
        embed.add_field(name="🔥 Chuỗi hiện tại", value=f"**{streak} ngày**", inline=True)
        embed.add_field(name="💳 Số dư mới", value=f"**{total_cash:,} VND**", inline=False)
        
        await ctx.send(embed=embed)

    @commands.command(name='cashboard')
    async def show_cashboard(self, ctx):
        """Show top richest players"""
        guild_id = str(ctx.guild.id)
        
        # Get all users from memory
        users_data = []
        for key, data in self.bot.user_cash_memory.items():
            if key.startswith(f"{guild_id}_"):
                user_id = key.split('_')[1]
                users_data.append((user_id, data.get('cash', 0)))
        
        if not users_data:
            await ctx.send("Chưa có ai trên bảng xếp hạng!")
            return
            
        sorted_users = sorted(users_data, key=lambda x: x[1], reverse=True)
        
        embed = discord.Embed(
            title="🏆 Bảng Xếp Hạng Đại Gia",
            description="Những người giàu nhất máy chủ",
            color=0xffd700
        )
        
        for i, (u_id, cash) in enumerate(sorted_users[:10]):
            try:
                user = await self.bot.fetch_user(int(u_id))
                rank = ["🥇", "🥈", "🥉"][i] if i < 3 else f"{i+1}."
                embed.add_field(name=f"{rank} {user.display_name}", value=f"💰 {cash:,} VND", inline=True)
            except:
                continue
                
        await ctx.send(embed=embed)

    @commands.command(name='tx')
    async def start_overunder(self, ctx):
        """Start a new Tài Xỉu game"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        
        if guild_id not in self.bot.overunder_games:
            self.bot.overunder_games[guild_id] = {}
            
        for g_id, g_data in self.bot.overunder_games[guild_id].items():
            if g_data['channel_id'] == channel_id and g_data['status'] == 'active':
                await ctx.send("Đang có một phiên Tài Xỉu diễn ra trong kênh này!")
                return

        game_id = f"tx_{int(datetime.utcnow().timestamp())}"
        end_time = datetime.utcnow() + timedelta(seconds=150)
        
        self.bot.overunder_games[guild_id][game_id] = {
            'channel_id': channel_id,
            'status': 'active',
            'end_time': end_time,
            'bets': [],
            'created_at': datetime.utcnow(),
            'end_task': None
        }
        
        embed = discord.Embed(
            title="🎲 TRÒ CHƠI TÀI XỈU MỚI!",
            description="Hãy đặt cược ngay để nhận thưởng!",
            color=0xffd700
        )
        embed.add_field(name="🔺 TÀI", value="Tổng 3 xúc xắc: **11 - 18**", inline=True)
        embed.add_field(name="🔻 XỈU", value="Tổng 3 xúc xắc: **3 - 10**", inline=True)
        embed.add_field(name="📝 Cách chơi", value="Dùng `?cuoc <tai/xiu> <tiền>` để đặt cược", inline=False)
        embed.set_footer(text="Vòng cược kết thúc sau 150 giây")
        
        await ctx.send(embed=embed)
        
        task = asyncio.create_task(self.bot._end_overunder_game(guild_id, game_id))
        self.bot.overunder_games[guild_id][game_id]['end_task'] = task

    @commands.command(name='cuoc')
    async def place_bet(self, ctx, side: str, amount: str):
        """Place a bet on Tài or Xỉu"""
        side = side.lower()
        if side not in ['tai', 'xiu']:
            await ctx.send("Vui lòng chọn `tai` hoặc `xiu`!")
            return
            
        guild_id = str(ctx.guild.id)
        user_id = str(ctx.author.id)
        channel_id = str(ctx.channel.id)
        
        try:
            bet_amount = self.parse_amount(amount)
        except ValueError:
            await ctx.send("Số tiền không hợp lệ!")
            return

        if bet_amount == -1:
            current_cash, _, _ = self.bot._get_user_cash(guild_id, user_id)
            bet_amount = current_cash

        # Check for active game
        active_game = None
        if guild_id in self.bot.overunder_games:
            for g_id, g_data in self.bot.overunder_games[guild_id].items():
                if g_data['channel_id'] == channel_id and g_data['status'] == 'active':
                    active_game = (g_id, g_data)
                    break

        if not active_game:
            await ctx.send("Không có game Tài Xỉu nào đang diễn ra!")
            return

        game_id, game_data = active_game
        
        # Check cash and place bet
        current_cash, _, _ = self.bot._get_user_cash(guild_id, user_id)
        if current_cash < bet_amount:
            await ctx.send(f"Bạn không đủ tiền! Số dư: {current_cash:,} VND")
            return

        # Deduct money
        self.bot._update_user_cash(guild_id, user_id, -bet_amount)
        
        bet_data = {
            'user_id': user_id,
            'username': ctx.author.display_name,
            'side': side,
            'amount': bet_amount
        }
        game_data['bets'].append(bet_data)
        
        await ctx.send(f"✅ **{ctx.author.display_name}** đã cược **{bet_amount:,} VND** vào **{side.upper()}**!")

    @commands.command(name='txshow')
    async def txshow(self, ctx):
        """Start auto-cycling games"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        key = f"{guild_id}_{channel_id}"
        self.bot.overunder_autocycle[key] = True
        await ctx.send("🔄 Đã bật chế độ tự động Tài Xỉu!")

    @commands.command(name='gamestop')
    async def gamestop(self, ctx):
        """Stop current game and auto-cycling"""
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        key = f"{guild_id}_{channel_id}"
        if key in self.bot.overunder_autocycle:
            del self.bot.overunder_autocycle[key]
        await ctx.send("⏹️ Đã dừng game và chế độ tự động!")

    @commands.command(name='win')
    @commands.has_permissions(administrator=True)
    async def set_winner(self, ctx, result: str):
        """Manually set the winner (Admin only)"""
        result = result.lower()
        if result not in ['tai', 'xiu']:
            await ctx.send("Chọn `tai` hoặc `xiu`!")
            return
            
        guild_id = str(ctx.guild.id)
        channel_id = str(ctx.channel.id)
        
        active_game = None
        if guild_id in self.bot.overunder_games:
            for g_id, g_data in self.bot.overunder_games[guild_id].items():
                if g_data['channel_id'] == channel_id and g_data['status'] == 'active':
                    active_game = (g_id, g_data)
                    break
                    
        if not active_game:
            await ctx.send("Không có game nào đang chạy!")
            return

        game_id, _ = active_game
        self.bot.manual_win_result = result
        await self.bot._end_overunder_game(guild_id, game_id, instant_stop=True)
        await ctx.send(f"✅ Đã đặt kết quả thắng là: **{result.upper()}**")

    @commands.command(name='give')
    async def give_money(self, ctx, user: discord.Member, amount: str):
        """Give money to another user"""
        if user.bot or user == ctx.author:
            await ctx.send("Không thể tặng tiền cho bản thân hoặc bot!")
            return

        guild_id = str(ctx.guild.id)
        try:
            give_amount = self.parse_amount(amount)
        except ValueError:
            await ctx.send("Số tiền không hợp lệ!")
            return

        giver_cash, _, _ = self.bot._get_user_cash(guild_id, str(ctx.author.id))
        if give_amount == -1:
            give_amount = giver_cash

        if giver_cash < give_amount:
            await ctx.send("Bạn không đủ tiền!")
            return

        self.bot._update_user_cash(guild_id, str(ctx.author.id), -give_amount)
        self.bot._update_user_cash(guild_id, str(user.id), give_amount)
        
        await ctx.send(f"✅ **{ctx.author.display_name}** đã tặng **{give_amount:,} VND** cho **{user.display_name}**!")

    @commands.command(name='moneyhack')
    @commands.has_permissions(administrator=True)
    async def moneyhack(self, ctx, amount: str, user: discord.Member = None):
        """Admin command to add money"""
        user = user or ctx.author
        try:
            hack_amount = self.parse_amount(amount)
        except ValueError:
            await ctx.send("Số tiền không hợp lệ!")
            return
            
        self.bot._update_user_cash(str(ctx.guild.id), str(user.id), hack_amount)
        await ctx.send(f"✅ Đã thêm **{hack_amount:,} VND** cho **{user.display_name}**!")

    @commands.command(name='clear')
    @commands.has_permissions(administrator=True)
    async def clear_money(self, ctx, user: discord.Member):
        """Admin command to reset money"""
        guild_id = str(ctx.guild.id)
        user_id = str(user.id)
        key = f"{guild_id}_{user_id}"
        if key in self.bot.user_cash_memory:
            self.bot.user_cash_memory[key]['cash'] = 0
            self.bot._save_backup_data()
            await ctx.send(f"✅ Đã reset tiền của **{user.display_name}** về 0!")
        else:
            await ctx.send(f"❌ Không tìm thấy dữ liệu của **{user.display_name}**!")

async def setup(bot):
    await bot.add_cog(TaiXiu(bot))
