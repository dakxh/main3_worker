import discord
import asyncio
import aiohttp
import re
import os
from datetime import datetime, timezone

# --- CONFIGURATION ---
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN") # The bot token (needs message read/manage messages intents)
MOVIE_QUEUE_CHANNEL_ID = int(os.getenv("MOVIE_QUEUE_CHANNEL_ID")) # e.g., 1234567890

GITHUB_PAT = os.getenv("GITHUB_PAT") # Classic Token with 'repo' scope
REPO_OWNER = os.getenv("REPO_OWNER") # e.g., "YourUsername"
REPO_NAME = os.getenv("REPO_NAME") # e.g., "YourRepo"
WORKFLOW_FILE = "main3.yml" # Name of the lobotomized workflow file

API_BASE = f"https://api.github.com/repos/{REPO_OWNER}/{REPO_NAME}/actions"
HEADERS = {
    "Authorization": f"Bearer {GITHUB_PAT}",
    "Accept": "application/vnd.github.v3+json"
}

# --- STATE ---
# This is our FIFO Waiting Room
processing_queue = asyncio.Queue()
# Ensure only one runner is actively monitored at a time for this specific pipeline
is_processing = False 

class PipelineWorker(discord.Client):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = None

    async def setup_hook(self):
        self.session = aiohttp.ClientSession(headers=HEADERS)
        self.bg_task = self.loop.create_task(self.process_queue_loop())

    async def on_ready(self):
        print(f"✅ {self.user} is online and watching #{MOVIE_QUEUE_CHANNEL_ID}")

    async def on_message(self, message):
        # Ignore our own messages
        if message.author == self.user:
            return
            
        # Only listen to the specific #movie-queue channel
        if message.channel.id == MOVIE_QUEUE_CHANNEL_ID:
            link = message.content.strip()
            if link.startswith("http") or link.startswith("magnet:"):
                # Add a tuple of (link, discord_message_object) to the FIFO queue
                await processing_queue.put((link, message))
                print(f"📥 Added to queue: {link[:30]}... (Queue size: {processing_queue.qsize()})")

    async def trigger_workflow(self, target_link):
        """Fires the POST request to trigger main3.yml"""
        url = f"{API_BASE}/workflows/{WORKFLOW_FILE}/dispatches"
        payload = {
            "ref": "main", # Or whatever your default branch is
            "inputs": {
                "target_link": target_link
            }
        }
        async with self.session.post(url, json=payload) as resp:
            if resp.status == 204:
                return True
            else:
                text = await resp.text()
                print(f"❌ Failed to trigger workflow: {resp.status} - {text}")
                return False

    async def get_latest_run_id(self, trigger_time):
        """Finds the Run ID of the workflow we just triggered"""
        url = f"{API_BASE}/runs?event=workflow_dispatch&per_page=5"
        # We poll a few times because GH API is sometimes a second behind
        for _ in range(5): 
            async with self.session.get(url) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    runs = data.get("workflow_runs", [])
                    if runs:
                        latest_run = runs[0]
                        # Verify it's the right workflow
                        if WORKFLOW_FILE in latest_run['path']:
                            return latest_run['id']
            await asyncio.sleep(2)
        return None

    async def cancel_run(self, run_id):
        """Cancels a hung run"""
        url = f"{API_BASE}/runs/{run_id}/cancel"
        async with self.session.post(url) as resp:
            return resp.status == 202

    async def get_job_steps(self, run_id):
        """Gets the current status of all steps in the active job"""
        url = f"{API_BASE}/runs/{run_id}/jobs"
        async with self.session.get(url) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data.get("jobs"):
                    # We assume 1 job ('process-queue')
                    return data["jobs"][0].get("steps", []), data["jobs"][0]["status"]
        return [], None

    async def process_queue_loop(self):
        """The main async loop that manages the State Machine"""
        global is_processing
        await self.wait_until_ready()

        while not self.is_closed():
            # Wait for an item in the queue. Blocks if empty, saving CPU.
            link, message = await processing_queue.get()
            is_processing = True
            
            success = False
            while not success:
                print(f"\n🚀 [STATE 0] Triggering Action for: {link[:30]}...")
                trigger_time = datetime.now(timezone.utc)
                if not await self.trigger_workflow(link):
                    await asyncio.sleep(10)
                    continue # Retry trigger

                await asyncio.sleep(5) # Wait for GH servers to register the run
                run_id = await self.get_latest_run_id(trigger_time)
                
                if not run_id:
                    print("⚠️ Could not find Run ID. Retrying in 10s...")
                    await asyncio.sleep(10)
                    continue
                    
                print(f"🔗 Attached to Run ID: {run_id}")
                
                # --- STATE 1: THE 5-MINUTE WATCHDOG ---
                timeout_time = asyncio.get_event_loop().time() + 90 # 5 minutes
                compile_started = False
                
                while asyncio.get_event_loop().time() < timeout_time:
                    steps, job_status = await self.get_job_steps(run_id)
                    
                    if job_status == "completed":
                        print("❌ Job crashed or finished too early during setup.")
                        break # Break out of watchdog to trigger a retry
                    
                    # Look for our Checkpoint 1 step
                    ack_step = next((s for s in steps if s['name'] == 'Acknowledge Payload'), None)
                    if ack_step and ack_step['status'] in ['in_progress', 'completed']:
                        compile_started = True
                        print("✅ [CHECKPOINT 1] Payload Acknowledged. Runner is healthy.")
                        break # Runner is good, exit watchdog
                        
                    await asyncio.sleep(15) # Poll every 15s

                if not compile_started:
                    print("🚨 RUNNER HANG DETECTED (5 minutes without reaching Checkpoint 1). Canceling...")
                    await self.cancel_run(run_id)
                    await asyncio.sleep(15)
                    print("🔄 Retrying same payload...")
                    continue # Loop back and trigger the same link again

                # --- CHECKPOINT 2: DOWNLOAD COMPLETION ---
                download_success = False
                while True:
                    steps, job_status = await self.get_job_steps(run_id)
                    dl_step = next((s for s in steps if s['name'] == 'Download and Extract Payload'), None)
                    
                    if dl_step and dl_step['status'] == 'completed':
                        if dl_step['conclusion'] == 'success':
                            print("✅ [CHECKPOINT 2] Download Complete.")
                            download_success = True
                            break
                        else:
                            print("❌ Download step failed. Retrying payload...")
                            break
                    
                    if job_status == "completed":
                         break
                         
                    await asyncio.sleep(15)

                if not download_success:
                    continue # Back to top of while not success loop

                # --- CHECKPOINT 3: THE 750s OVERLAP TIMER ---
                timer_started = False
                while True:
                    steps, job_status = await self.get_job_steps(run_id)
                    master_step = next((s for s in steps if s['name'] == 'Compile Pipeline Worker (Phase 2 - Muxing & Execution)'), None)
                    
                    if master_step and master_step['status'] in ['in_progress', 'completed']:
                        print("⏱️ [CHECKPOINT 3] Master Loop started. Commencing 750s overlap countdown...")
                        timer_started = True
                        break
                        
                    if job_status == "completed":
                        break
                        
                    await asyncio.sleep(15)
                    
                if timer_started:
                    await asyncio.sleep(66)
                    print("🟢 [SUCCESS] 750s passed. Marking link as safely processing.")
                    
                    # --- CLEANUP PHASE ---
                    try:
                        await message.delete()
                        print("🗑️ Deleted original Discord message.")
                    except discord.Forbidden:
                        print("⚠️ Cannot delete message: Bot lacks permissions.")
                    except discord.NotFound:
                        pass
                        
                    success = True # This exits the retry loop and moves to the next item in the Queue
                else:
                    print("❌ Run failed before reaching Master Loop. Retrying payload...")

            # Free up the slot for the next link in the queue
            is_processing = False
            print(f"⏭️ Ready for next item. Queue size: {processing_queue.qsize()}")

# Run the Bot
intents = discord.Intents.default()
intents.message_content = True
client = PipelineWorker(intents=intents)

if __name__ == "__main__":
    client.run(DISCORD_BOT_TOKEN)
