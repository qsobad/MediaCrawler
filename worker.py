#!/usr/bin/env python3
"""
MediaCrawler Worker - API Mode
Accepts crawl requests via HTTP API
"""

import asyncio
import json
import os
import sys
import uuid
from datetime import datetime
from typing import Optional, List
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# Add MediaCrawler to path
CRAWLER_PATH = os.getenv("CRAWLER_PATH", "/app/MediaCrawler")
sys.path.insert(0, CRAWLER_PATH)

app = FastAPI(
    title="MediaCrawler Worker",
    description="API for crawling social media content",
    version="1.0.0"
)

# ============== Models ==============

class CrawlRequest(BaseModel):
    """Request to crawl content"""
    platform: str = Field(..., description="Platform: dy, xhs, ks, bili, wb")
    task_type: str = Field(default="detail", description="Task type: search, detail, creator")
    urls: Optional[List[str]] = Field(default=None, description="URLs to crawl (for detail/creator)")
    keywords: Optional[List[str]] = Field(default=None, description="Keywords (for search)")
    cookies: Optional[str] = Field(default=None, description="Cookie string for authentication")
    max_items: int = Field(default=10, description="Maximum items to crawl")
    
class CrawlResponse(BaseModel):
    """Response from crawl request"""
    task_id: str
    status: str
    message: str

class TaskStatus(BaseModel):
    """Status of a crawl task"""
    task_id: str
    status: str  # pending, running, completed, failed
    progress: int
    result: Optional[dict] = None
    error: Optional[str] = None
    created_at: str
    updated_at: str

# ============== Task Storage ==============

tasks = {}  # In-memory storage, use Redis in production

def update_task(task_id: str, **kwargs):
    if task_id in tasks:
        tasks[task_id].update(kwargs)
        tasks[task_id]["updated_at"] = datetime.now().isoformat()

# ============== Crawler Logic ==============

async def run_crawler(task_id: str, request: CrawlRequest):
    """Run the MediaCrawler for a given request"""
    try:
        update_task(task_id, status="running", progress=10)
        
        # Import MediaCrawler components
        from config import base_config
        
        # Configure crawler
        base_config.PLATFORM = request.platform
        base_config.CRAWLER_TYPE = request.task_type
        base_config.HEADLESS = True
        
        if request.cookies:
            base_config.LOGIN_TYPE = "cookie"
            base_config.COOKIES = request.cookies
        
        if request.keywords:
            base_config.KEYWORDS = ",".join(request.keywords)
        
        update_task(task_id, progress=20)
        
        # Import and run platform-specific crawler
        if request.platform == "dy":
            from media_platform.douyin import DouYinCrawler
            crawler = DouYinCrawler()
        elif request.platform == "xhs":
            from media_platform.xhs import XiaoHongShuCrawler
            crawler = XiaoHongShuCrawler()
        elif request.platform == "ks":
            from media_platform.kuaishou import KuaishouCrawler
            crawler = KuaishouCrawler()
        elif request.platform == "bili":
            from media_platform.bilibili import BilibiliCrawler
            crawler = BilibiliCrawler()
        elif request.platform == "wb":
            from media_platform.weibo import WeiboCrawler
            crawler = WeiboCrawler()
        else:
            raise ValueError(f"Unsupported platform: {request.platform}")
        
        update_task(task_id, progress=30)
        
        # Run crawler
        await crawler.start()
        
        update_task(task_id, progress=80)
        
        # Collect results
        data_dir = Path(CRAWLER_PATH) / "data" / request.platform
        results = []
        
        if data_dir.exists():
            for json_file in data_dir.glob("*.json"):
                with open(json_file, "r", encoding="utf-8") as f:
                    results.extend(json.load(f) if json_file.stat().st_size > 0 else [])
        
        update_task(
            task_id, 
            status="completed", 
            progress=100,
            result={"items": results[:request.max_items], "total": len(results)}
        )
        
    except Exception as e:
        update_task(task_id, status="failed", error=str(e))
        raise

# ============== API Endpoints ==============

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "timestamp": datetime.now().isoformat()}

@app.get("/")
async def root():
    """Root endpoint with API info"""
    return {
        "name": "MediaCrawler Worker",
        "version": "1.0.0",
        "endpoints": {
            "POST /crawl": "Submit a crawl request",
            "GET /task/{task_id}": "Get task status",
            "GET /tasks": "List all tasks",
            "GET /health": "Health check"
        }
    }

@app.post("/crawl", response_model=CrawlResponse)
async def submit_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    """Submit a new crawl request"""
    task_id = str(uuid.uuid4())[:8]
    
    tasks[task_id] = {
        "task_id": task_id,
        "status": "pending",
        "progress": 0,
        "result": None,
        "error": None,
        "request": request.dict(),
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat()
    }
    
    background_tasks.add_task(run_crawler, task_id, request)
    
    return CrawlResponse(
        task_id=task_id,
        status="pending",
        message=f"Crawl task submitted for {request.platform}"
    )

@app.get("/task/{task_id}", response_model=TaskStatus)
async def get_task(task_id: str):
    """Get status of a crawl task"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    task = tasks[task_id]
    return TaskStatus(
        task_id=task["task_id"],
        status=task["status"],
        progress=task["progress"],
        result=task.get("result"),
        error=task.get("error"),
        created_at=task["created_at"],
        updated_at=task["updated_at"]
    )

@app.get("/tasks")
async def list_tasks():
    """List all tasks"""
    return {
        "tasks": [
            {
                "task_id": t["task_id"],
                "status": t["status"],
                "progress": t["progress"],
                "platform": t["request"]["platform"],
                "created_at": t["created_at"]
            }
            for t in tasks.values()
        ]
    }

@app.delete("/task/{task_id}")
async def delete_task(task_id: str):
    """Delete a task"""
    if task_id not in tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    
    del tasks[task_id]
    return {"message": f"Task {task_id} deleted"}

# ============== Main ==============

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    host = os.getenv("HOST", "0.0.0.0")
    
    print(f"Starting MediaCrawler Worker on {host}:{port}")
    uvicorn.run(app, host=host, port=port)
