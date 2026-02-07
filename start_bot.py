#!/usr/bin/env python3
"""
Start Discord Bot Cell for Google Colab
Run this in a SEPARATE cell after the keep-alive cell is running
"""

import os
import sys
import subprocess
import threading
import time
from IPython.display import display, HTML, clear_output
from datetime import datetime

def check_token():
    """Check if Discord bot token is set"""
    token = os.environ.get('DISCORD_BOT_TOKEN')
    if not token:
        display(HTML('''
        <div style="background: #ff4444; color: white; padding: 15px; border-radius: 8px; text-align: center; margin: 10px 0;">
            <h3>❌ Discord Bot Token Required!</h3>
            <p><strong>Set your token first:</strong></p>
            <code style="background: rgba(255,255,255,0.2); padding: 5px; border-radius: 3px;">
                os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_TOKEN_HERE'
            </code>
            <p style="margin-top: 10px;">
                <a href="https://discord.com/developers/applications" target="_blank" 
                   style="color: white; text-decoration: underline;">
                    Get your token from Discord Developer Portal
                </a>
            </p>
        </div>
        '''))
        return False
    return True

def start_bot_process():
    """Start Discord bot as a background process with live output"""
    
    if not check_token():
        return False
    
    display(HTML('''
    <div style="background: linear-gradient(45deg, #1e3c72, #2a5298); color: white; padding: 15px; border-radius: 10px; text-align: center; margin: 10px 0;">
        <h3>🚀 Starting Discord Bot...</h3>
        <p>💚 Keep-alive protection is active</p>
        <p>🤖 Bot will run in background with live logs</p>
    </div>
    '''))
    
    print("🚀 Starting Discord Bot process...")
    print("💚 Keep-alive system should be running in another cell")
    print("📋 Live bot output below:")
    print("=" * 60)
    
    try:
        # Start bot as subprocess with live output
        # Always run main.py which now contains all logic
        process = subprocess.Popen(
            [sys.executable, 'main.py'],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        print(f"🤖 Bot process started with PID: {process.pid}")
        
        # Create a thread to read output continuously
        def read_output():
            try:
                for line in iter(process.stdout.readline, ''):
                    if line.strip():
                        # Color-code important messages
                        line = line.strip()
                        timestamp = datetime.now().strftime("%H:%M:%S")
                        
                        if "connected to Discord" in line.lower():
                            print(f"🟢 [{timestamp}] {line}")
                        elif "error" in line.lower():
                            print(f"🔴 [{timestamp}] {line}")
                        elif "warning" in line.lower():
                            print(f"🟡 [{timestamp}] {line}")
                        elif "still alive" in line.lower():
                            print(f"💚 [{timestamp}] {line}")
                        else:
                            print(f"ℹ️ [{timestamp}] {line}")
                            
                process.wait()
                print(f"\n🛑 Bot process ended with code: {process.returncode}")
                
            except Exception as e:
                print(f"❌ Error reading bot output: {e}")
        
        # Start output reading thread
        output_thread = threading.Thread(target=read_output, daemon=True)
        output_thread.start()
        
        # Status display thread
        def status_display():
            start_time = time.time()
            while process.poll() is None:  # While process is running
                runtime = int(time.time() - start_time)
                hours = runtime // 3600
                minutes = (runtime % 3600) // 60
                seconds = runtime % 60
                
                # Update status every 5 minutes
                if runtime % 300 == 0 and runtime > 0:
                    print(f"\n📊 Bot Status Update:")
                    print(f"   ⏱️ Runtime: {hours:02d}:{minutes:02d}:{seconds:02d}")
                    print(f"   🤖 Process ID: {process.pid}")
                    print(f"   💚 Keep-alive should be active in other cell")
                    print(f"   📱 Session protected for up to 12 hours")
                    print("=" * 40)
                
                time.sleep(30)  # Check every 30 seconds
        
        # Start status thread
        status_thread = threading.Thread(target=status_display, daemon=True)
        status_thread.start()
        
        print("\n✅ Bot started successfully!")
        print("💡 To stop the bot: Press Ctrl+C or restart this cell")
        print("📊 Status updates every 5 minutes")
        print("💚 Keep the keep-alive cell running in the other cell!")
        
        # Keep this cell alive to show output
        try:
            # Wait for process to finish or user interrupt
            while process.poll() is None:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n🛑 Stopping bot...")
            process.terminate()
            try:
                process.wait(timeout=10)
                print("✅ Bot stopped cleanly")
            except subprocess.TimeoutExpired:
                process.kill()
                print("🔪 Bot force-killed")
        
        return True
        
    except Exception as e:
        print(f"❌ Error starting bot: {e}")
        return False

def quick_token_setup():
    """Helper function to quickly set token"""
    print("🔑 Quick token setup:")
    print("Paste your Discord bot token below (it will be hidden)")
    print("Get it from: https://discord.com/developers/applications")
    print("")
    print("Run this command:")
    print("import os")
    print("os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_DISCORD_BOT_TOKEN_HERE'")
    print("")
    print("Then run: start_bot_process()")

# Check if this is being run directly
if __name__ == "__main__":
    # Auto-start if token is available
    if os.environ.get('DISCORD_BOT_TOKEN'):
        start_bot_process()
    else:
        quick_token_setup()
else:
    # When imported, show instructions
    display(HTML('''
    <div style="background: linear-gradient(45deg, #667eea, #764ba2); color: white; padding: 20px; border-radius: 10px; margin: 10px 0;">
        <h2>🤖 Discord Bot Starter Ready!</h2>
        <div style="text-align: left; margin: 15px 0;">
            <h3>📋 Instructions:</h3>
            <ol style="line-height: 1.8;">
                <li><strong>Set your token:</strong><br>
                    <code style="background: rgba(255,255,255,0.2); padding: 5px; border-radius: 3px;">
                        os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_TOKEN_HERE'
                    </code>
                </li>
                <li><strong>Start the bot:</strong><br>
                    <code style="background: rgba(255,255,255,0.2); padding: 5px; border-radius: 3px;">
                        start_bot_process()
                    </code>
                </li>
            </ol>
        </div>
        <p style="text-align: center; margin-top: 15px; font-size: 14px;">
            💚 Make sure your keep-alive cell is running first!
        </p>
    </div>
    '''))
    
    print("🎯 Discord Bot starter loaded!")
    print("📋 Available functions:")
    print("   • start_bot_process() - Start the Discord bot")  
    print("   • quick_token_setup() - Show token setup help")
    print("")
    print("💚 Ensure keep-alive cell is running first!")