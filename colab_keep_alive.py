#!/usr/bin/env python3
"""
Continuous Keep-Alive Cell for Google Colab
This cell runs continuously while allowing other cells to execute
"""

import time
import threading
from IPython.display import display, HTML, Javascript, clear_output
from datetime import datetime
import asyncio

class ColabKeepAlive:
    def __init__(self):
        self.running = True
        self.counter = 0
        self.thread = None
        
    def start_continuous_display(self):
        """Run continuous keep-alive with live updates - NON-BLOCKING"""
        
        # Start JavaScript keep-alive immediately (non-blocking)
        display(Javascript('''
            console.log("🤖 Starting Colab Keep-Alive System");
            
            function colabKeepAlive() {
                console.log("💚 Keep-Alive Active:", new Date().toLocaleTimeString());
                
                // Simulate user interactions
                document.body.click();
                window.scrollBy(0, 1);
                window.scrollBy(0, -1);
                
                // Try to click connect button if disconnected
                const connectBtn = document.querySelector('#top-toolbar > colab-connect-button');
                if (connectBtn && connectBtn.shadowRoot) {
                    const btn = connectBtn.shadowRoot.querySelector('#connect');
                    if (btn && btn.textContent === 'Connect') {
                        console.log("🔌 Auto-reconnecting...");
                        btn.click();
                    }
                }
                
                // Trigger mouse events to prevent idle
                document.body.dispatchEvent(new MouseEvent('mousemove', {
                    view: window,
                    bubbles: true,
                    cancelable: true
                }));
            }
            
            // Clear any existing intervals
            if (window.colabKeepAliveInterval) {
                clearInterval(window.colabKeepAliveInterval);
            }
            
            // Run every 90 seconds for optimal protection
            window.colabKeepAliveInterval = setInterval(colabKeepAlive, 90000);
            
            console.log("✅ Keep-alive started! Running every 90 seconds.");
        '''))
        
        # Start background thread for visual updates
        def update_display():
            while self.running:
                try:
                    self.counter += 1
                    current_time = datetime.now().strftime("%H:%M:%S")
                    
                    # Clear and update display every minute
                    clear_output(wait=True)
                    
                    # Show current status
                    display(HTML(f'''
                    <div style="
                        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
                        color: white;
                        padding: 20px;
                        border-radius: 15px;
                        text-align: center;
                        font-family: 'Segoe UI', Arial, sans-serif;
                        box-shadow: 0 8px 32px rgba(0,0,0,0.3);
                        margin: 10px 0;
                        border: 1px solid rgba(255,255,255,0.2);
                    ">
                        <h2 style="margin: 0 0 10px 0; font-size: 24px;">🤖 Discord Bot Keep-Alive</h2>
                        <div style="display: flex; justify-content: space-around; margin: 15px 0;">
                            <div style="text-align: center;">
                                <div style="font-size: 28px; font-weight: bold; color: #4CAF50;">#{self.counter}</div>
                                <div style="font-size: 12px; opacity: 0.9;">Heartbeats</div>
                            </div>
                            <div style="text-align: center;">
                                <div style="font-size: 20px; font-weight: bold;">⏰ {current_time}</div>
                                <div style="font-size: 12px; opacity: 0.9;">Current Time</div>
                            </div>
                            <div style="text-align: center;">
                                <div style="font-size: 20px; font-weight: bold; color: #4CAF50;">🟢 ACTIVE</div>
                                <div style="font-size: 12px; opacity: 0.9;">Session Status</div>
                            </div>
                        </div>
                        <div style="
                            background: rgba(255,255,255,0.1);
                            padding: 10px;
                            border-radius: 8px;
                            margin: 10px 0;
                            font-size: 14px;
                        ">
                            💡 <strong>Keep-Alive Protection:</strong> JavaScript + Python threads running<br>
                            🔄 <strong>Next Update:</strong> 60 seconds | 
                            🤖 <strong>Ready for Bot:</strong> Start your Discord bot in another cell!
                        </div>
                        <div style="font-size: 12px; opacity: 0.8; margin-top: 10px;">
                            This cell keeps running - you can execute other cells normally
                        </div>
                    </div>
                    '''))
                    
                    # Console output for verification
                    print(f"💚 Keep-Alive #{self.counter} at {current_time} - Session Protected! Other cells can run normally.")
                    
                    # Wait 60 seconds before next update
                    time.sleep(60)
                    
                except KeyboardInterrupt:
                    print("🛑 Keep-alive stopped by user")
                    self.running = False
                    break
                except Exception as e:
                    print(f"⚠️ Keep-alive error: {e}")
                    time.sleep(60)  # Continue despite errors
        
        # Start the background thread
        self.thread = threading.Thread(target=update_display, daemon=True)
        self.thread.start()
        
        # Show initial status
        display(HTML('''
        <div style="background: #4CAF50; color: white; padding: 15px; border-radius: 8px; text-align: center; margin: 10px 0;">
            <h3>✅ Keep-Alive System Started!</h3>
            <p><strong>📱 This cell will keep running continuously</strong></p>
            <p>🚀 You can now run your Discord bot in another cell!</p>
            <p>💡 Both will work simultaneously - no blocking!</p>
        </div>
        '''))
        
        print("✅ Continuous keep-alive started!")
        print("📱 This cell will update every 60 seconds")
        print("🚀 You can run other cells normally!")
        print("🤖 Start your Discord bot with: exec(open('start_bot.py').read())")
        
    def stop(self):
        """Stop the keep-alive system"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2)
        print("🛑 Keep-alive system stopped")

# Create global instance
if 'colab_keeper' not in globals():
    colab_keeper = ColabKeepAlive()

# Auto-start the continuous keep-alive
try:
    colab_keeper.start_continuous_display()
    
    # This creates a continuous loop that shows updates but doesn't block
    # The secret is using daemon threads + JavaScript intervals
    print("🎯 Keep-alive cell is running! Execute your bot in another cell.")
    
except KeyboardInterrupt:
    print("🛑 Keep-alive interrupted by user")
    colab_keeper.stop()
except Exception as e:
    print(f"❌ Keep-alive error: {e}")
    print("💡 Try restarting this cell")