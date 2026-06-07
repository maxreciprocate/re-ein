from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from contextlib import asynccontextmanager
from jupyter_client import KernelManager
import asyncio
import json
import queue
import logging
import threading
import time
from typing import Callable, List, Dict, Any, Optional
import signal
import sys

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

km: KernelManager = None
kc = None
execute_lock = threading.Lock()


class CodeRequest(BaseModel):
    code: str
    timeout: float = 86400.0


class OutputResponse(BaseModel):
    outputs: List[Dict[str, Any]]
    status: str


def _msg_to_output(msg: dict) -> Optional[Dict[str, Any]]:
    """Translate a kernel iopub message to our output dict, or None to skip,
    or the string 'done' when execution finishes."""
    msg_type = msg['header']['msg_type']
    content = msg['content']
    if msg_type == 'stream':
        return {'type': 'stream', 'name': content['name'], 'text': content['text']}
    if msg_type == 'execute_result':
        return {
            'type': 'execute_result',
            'data': content['data'],
            'execution_count': content['execution_count'],
        }
    if msg_type == 'display_data':
        return {
            'type': 'display_data',
            'data': content['data'],
            'metadata': content.get('metadata', {}),
        }
    if msg_type == 'error':
        return {
            'type': 'error',
            'ename': content['ename'],
            'evalue': content['evalue'],
            'traceback': content['traceback'],
        }
    if msg_type == 'status' and content['execution_state'] == 'idle':
        return 'done'
    return None


def _drain_until_idle(msg_id: str, grace: float = 5.0) -> None:
    """After an interrupt, consume the kernel's remaining iopub messages for
    this execution so they don't bleed into the next request."""
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline:
        try:
            msg = kc.get_iopub_msg(timeout=0.2)
        except queue.Empty:
            continue
        if (msg['parent_header'].get('msg_id') == msg_id
                and msg['header']['msg_type'] == 'status'
                and msg['content']['execution_state'] == 'idle'):
            return


def _run_execution(code: str, timeout: float,
                   on_output: Callable[[Dict[str, Any]], None]) -> None:
    """Run code in the kernel; call on_output(dict) for each output as it
    arrives. Blocks until execution completes, errors, or times out."""
    if not kc:
        raise HTTPException(status_code=500, detail="Kernel not initialized")

    with execute_lock:
        msg_id = kc.execute(code)
        deadline = time.monotonic() + timeout

        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                km.interrupt_kernel()
                _drain_until_idle(msg_id)
                on_output({
                    'type': 'error',
                    'ename': 'TimeoutError',
                    'evalue': f'Code execution exceeded timeout of {timeout} seconds',
                    'traceback': [],
                })
                return

            try:
                msg = kc.get_iopub_msg(timeout=min(remaining, 1.0))
            except queue.Empty:
                continue

            if msg['parent_header'].get('msg_id') != msg_id:
                continue

            out = _msg_to_output(msg)
            if out == 'done':
                return
            if out is not None:
                on_output(out)


def execute_code(code: str, timeout: float) -> List[Dict[str, Any]]:
    outputs: List[Dict[str, Any]] = []
    _run_execution(code, timeout, outputs.append)
    return outputs


@asynccontextmanager
async def lifespan(app: FastAPI):
    global km, kc

    logger.info("Starting Jupyter kernel...")
    km = KernelManager()
    km.start_kernel()
    kc = km.client()
    kc.start_channels()
    kc.wait_for_ready(timeout=10)
    logger.info("Kernel started successfully")

    yield

    logger.info("Shutting down kernel...")
    kc.stop_channels()
    km.shutdown_kernel()
    logger.info("Kernel shut down")


app = FastAPI(title="Jupyter Kernel API", lifespan=lifespan)


@app.get("/")
async def root():
    return {"status": "running", "kernel_alive": km.is_alive() if km else False}


@app.post("/execute", response_model=OutputResponse)
async def execute(request: CodeRequest):
    try:
        outputs = execute_code(request.code, request.timeout)
        return OutputResponse(outputs=outputs, status="success")
    except Exception as e:
        logger.error(f"Execution failed: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/execute/stream")
async def execute_stream(request: CodeRequest):
    """Stream outputs as NDJSON, one output per line, flushed as they arrive
    from the kernel. Connection closes when execution completes."""
    q: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue()
    SENTINEL = object()

    def runner():
        try:
            _run_execution(request.code, request.timeout, q.put)
        except Exception as e:
            q.put({
                'type': 'error',
                'ename': 'ExecutionError',
                'evalue': str(e),
                'traceback': [],
            })
        finally:
            q.put(SENTINEL)

    threading.Thread(target=runner, daemon=True).start()

    async def gen():
        while True:
            item = await asyncio.to_thread(q.get)
            if item is SENTINEL:
                return
            yield json.dumps(item) + "\n"

    return StreamingResponse(gen(), media_type="application/x-ndjson")


@app.get("/kernel/status")
async def kernel_status():
    if not km:
        return {"status": "not_initialized"}
    return {
        "status": "running" if km.is_alive() else "dead",
        "connection_info": {
            "transport": km.transport,
            "ip": km.ip,
            "shell_port": km.shell_port,
            "iopub_port": km.iopub_port,
        },
    }


@app.post("/kernel/restart")
async def restart_kernel():
    global km, kc
    if not km:
        raise HTTPException(status_code=500, detail="Kernel not initialized")

    logger.info("Restarting kernel...")
    with execute_lock:
        kc.stop_channels()
        km.shutdown_kernel()
        km = KernelManager()
        km.start_kernel()
        kc = km.client()
        kc.start_channels()
        kc.wait_for_ready(timeout=10)
    logger.info("Kernel restarted successfully")

    return {"status": "restarted"}


@app.post("/kernel/interrupt")
async def interrupt_kernel():
    if not km:
        raise HTTPException(status_code=500, detail="Kernel not initialized")
    km.interrupt_kernel()
    return {"status": "interrupted"}


def signal_handler(sig, frame):
    logger.info("Received shutdown signal")
    if km and km.is_alive():
        kc.stop_channels()
        km.shutdown_kernel()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
