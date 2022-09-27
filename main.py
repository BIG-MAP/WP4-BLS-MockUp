import asyncio
from enum import Enum

import pandas as pd
from fastapi import FastAPI

app = FastAPI()


class Status(Enum):
    idle = "idle"
    running = "running"
    finished = "finished"
    stopped = "stopped"


class LiquidExtractor:
    status: Status = Status.idle
    results: pd.DataFrame = pd.DataFrame()

    _loop = asyncio.get_event_loop()

    def start(self):
        print("Starting")
        self.status = Status.running
        self.results = pd.DataFrame()
        self._loop.create_task(self.finish())

    async def finish(self):
        await asyncio.sleep(15)
        print("Finished")
        self.results = pd.read_csv("data.csv")
        self.status = Status.finished

    def stop(self):
        print("Stopping")
        self.status = Status.stopped
        self.results = pd.DataFrame()


extractor = LiquidExtractor()


@app.get("/")
def get_root():
    return {"Hello": "World"}


@app.get("/start")
def do_start():
    extractor.start()
    return {"status": extractor.status.value}


@app.get("/status")
def get_status():
    return {"status": extractor.status.value}


@app.get("/stop")
def do_stop():
    extractor.stop()
    return {"status": extractor.status.value}


@app.get("/results")
def get_results():
    return {"results": extractor.results.to_numpy().tolist()}
