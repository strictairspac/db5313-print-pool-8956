import logging
from pathlib import Path
from threading import Event, Thread
from time import monotonic, sleep

from src.config.settings import ARTIFACTS, cache, engine
from src.services import handovers
from src.services.domain import solve
from src.services.workflow import claim, finish

HEARTBEAT = Path(ARTIFACTS) / 'worker-heartbeat'
RETRY_INTERVAL = 20


def heartbeat(stop):
    while not stop.is_set():
        HEARTBEAT.touch()
        stop.wait(2)


def retry_handovers(last):
    """定期重发未收到回执的队首批次，补打系统恢复后自动续传。"""
    if monotonic() - last < RETRY_INTERVAL:
        return last
    try:
        handovers.retry_pending()
    except Exception:
        logging.exception('Handover retry failed')
    return monotonic()


def work():
    HEARTBEAT.unlink(missing_ok=True)
    last_retry = monotonic()
    while True:
        try:
            job = claim()
            HEARTBEAT.touch()
            last_retry = retry_handovers(last_retry)
            if not job:
                cache.brpop('worker:wake', timeout=1)
                continue
            run_id, token, payload = job
            stop = Event()
            thread = Thread(target=heartbeat, args=(stop,), daemon=True)
            thread.start()
            try:
                result = solve(payload, engine)
                finish(run_id, token, result=result)
            except (ValueError, KeyError, TypeError) as exc:
                finish(run_id, token, error=str(exc)[:500])
            except Exception:
                logging.exception('Domain run failed: %s', run_id)
                finish(run_id, token, error='后台执行失败，请检查输入或查看 worker 日志。')
            finally:
                stop.set()
                thread.join()
        except Exception:
            logging.exception('Worker loop failure')
            sleep(2)


if __name__ == '__main__':
    work()
