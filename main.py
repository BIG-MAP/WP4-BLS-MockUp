import asyncio
from enum import Enum
from typing import List

import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()


class Status(Enum):
    idle = "idle"
    running = "running"
    finished = "finished"
    stopped = "stopped"


class Settings(BaseModel):
    sleep_delay: int = 15


class LiquidExtractor:
    status: Status = Status.idle
    results: pd.DataFrame = pd.DataFrame()
    settings: Settings = Settings()

    _loop = asyncio.get_running_loop()
    _task: asyncio.Task = None

    def start(self, settings):
        print("Starting")
        self.settings = settings
        self.status = Status.running
        self.results = pd.DataFrame()
        self._task = self._loop.create_task(self.finish(settings.sleep_delay))

    async def finish(self, delay):
        await asyncio.sleep(delay)
        print("Finished")
        self.results = pd.read_csv("data.csv")
        self.status = Status.finished

    def stop(self):
        print("Stopping")
        self._task.cancel()
        self.status = Status.stopped
        self.results = pd.DataFrame()


class Response(BaseModel):
    status: Status
    settings: Settings | None = None
    results: List[List[int]] | None = None


extractor = LiquidExtractor()


@app.get("/")
def get_root():
    return {"Hello": "World"}


@app.get("/start")
def do_start():
    settings = Settings(sleep_delay=15)
    extractor.start(settings)
    return Response(status=extractor.status, settings=extractor.settings)


@app.post("/start")
def post_start(settings: Settings):
    extractor.start(settings)
    return Response(status=extractor.status, settings=extractor.settings)


@app.get("/status")
def get_status():
    return Response(status=extractor.status, settings=extractor.settings)


@app.get("/stop")
def do_stop():
    extractor.stop()
    return Response(status=extractor.status, settings=extractor.settings)


@app.get("/results")
def get_results():
    return Response(status=extractor.status, settings=extractor.settings, results=extractor.results.to_numpy().tolist())
