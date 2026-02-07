#!/usr/bin/env python3
"""
NON-BLOCKING Colab Setup
This sets up the environment but doesn't block other cells
"""

def colab_setup():
    """Quick setup for Colab environment - NON-BLOCKING"""
    
    print("🚀 Discord Bot Colab Setup (Non-Blocking)")
    print("=" * 50)
    
    # Step 1: Install packages
    print("📦 Installing packages...")
    import subprocess
    import sys
    
    try:
        result = subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print("✅ Packages installed from requirements.txt!")
        else:
            raise subprocess.CalledProcessError(result.returncode, "pip install")
    except:
        print("⚠️ Installing individual packages...")
        packages = ['discord.py>=2.3.0', 'openai>=1.0.0', 'flask>=2.0.0', 'nest-asyncio', 'IPython', 'jupyter']
        for pkg in packages:
            try:
                subprocess.check_call([sys.executable, "-m", "pip", "install", pkg], 
                                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                print(f"   ✓ {pkg}")
            except:
                print(f"   ⚠ {pkg} - may already be installed")
    
    # Step 2: Setup environment
    print("🔧 Setting up environment...")
    import os
    os.environ['COLAB_ENVIRONMENT'] = 'true'
    os.environ['PYTHONUNBUFFERED'] = '1'
    os.environ['DATABASE_URL'] = '' # Force empty to use JSON backup in Colab
    os.makedirs('logs', exist_ok=True)
    os.makedirs('configs', exist_ok=True)
    
    # Step 3: Show completion
    from IPython.display import display, HTML
    
    display(HTML('''
    <div style="background: linear-gradient(45deg, #667eea, #764ba2); color: white; padding: 20px; border-radius: 15px; text-align: center; margin: 10px 0;">
        <h2>✅ Colab Setup Complete!</h2>
        <h3>🎯 Ready for 2-Cell Discord Bot Deployment</h3>
        
        <div style="background: rgba(255,255,255,0.15); padding: 15px; border-radius: 10px; margin: 15px 0;">
            <h4>📋 Next Steps (2 Separate Cells):</h4>
            
            <div style="text-align: left; max-width: 500px; margin: 0 auto;">
                <div style="background: rgba(255,255,255,0.1); padding: 10px; border-radius: 5px; margin: 10px 0;">
                    <strong>🔄 Cell 1 - Keep-Alive (Continuous):</strong><br>
                    <code style="font-size: 14px;">exec(open('colab_keep_alive.py').read())</code>
                </div>
                
                <div style="background: rgba(255,255,255,0.1); padding: 10px; border-radius: 5px; margin: 10px 0;">
                    <strong>🤖 Cell 2 - Discord Bot:</strong><br>
                    <code style="font-size: 14px;">
                        import os<br>
                        os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_TOKEN'<br>
                        exec(open('start_bot.py').read())
                    </code>
                </div>
            </div>
        </div>
        
        <div style="font-size: 14px; margin-top: 15px; line-height: 1.6;">
            ✅ <strong>Non-Blocking Setup</strong> - You can run other cells<br>
            💚 <strong>Continuous Keep-Alive</strong> - 12+ hour protection<br>
            📱 <strong>Mobile Compatible</strong> - Works on phones/tablets
        </div>
    </div>
    '''))
    
    print("=" * 50)
    print("🎯 Setup Complete! Now run these in SEPARATE cells:")
    print("")
    print("📍 CELL 1 (Keep-Alive - runs continuously):")
    print("   exec(open('colab_keep_alive.py').read())")
    print("")
    print("📍 CELL 2 (Discord Bot - after Cell 1 is running):")
    print("   import os")
    print("   os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_DISCORD_TOKEN'")
    print("   exec(open('start_bot.py').read())")
    print("")
    print("💡 Both cells will run simultaneously without blocking!")

def quick_start():
    """Show quick start instructions"""
    from IPython.display import display, HTML
    
    display(HTML('''
    <div style="background: #1a73e8; color: white; padding: 15px; border-radius: 10px; text-align: center;">
        <h3>⚡ Quick Start Instructions</h3>
        <p>Run these commands in separate cells:</p>
        
        <div style="text-align: left; max-width: 400px; margin: 15px auto;">
            <div style="margin: 10px 0;">
                <strong>Cell 1 (Keep-Alive):</strong><br>
                <code>exec(open('colab_keep_alive.py').read())</code>
            </div>
            
            <div style="margin: 10px 0;">
                <strong>Cell 2 (Set Token & Start Bot):</strong><br>
                <code>
                    import os<br>
                    os.environ['DISCORD_BOT_TOKEN'] = 'YOUR_TOKEN'<br>
                    exec(open('start_bot.py').read())
                </code>
            </div>
        </div>
    </div>
    '''))

# Show instructions when imported
if __name__ == "__main__":
    colab_setup()
else:
    # Auto-run setup when imported
    colab_setup()