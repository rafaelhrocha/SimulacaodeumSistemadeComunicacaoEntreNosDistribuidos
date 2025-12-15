from __future__ import annotations

from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


class MessageType(str, Enum):
    HEARTBEAT = "heartbeat"
    ELECTION = "election"
    ELECTION_OK = "election_ok"
    LEADER_ANNOUNCE = "leader_announce"
    TIME_REQUEST = "time_request"
    TIME_REPLY = "time_reply"
    REQUEST_CS = "request_cs"
    REPLY_CS = "reply_cs"
    RELEASE_CS = "release_cs"
    ENQUEUE = "enqueue"
    ENQUEUE_ACK = "enqueue_ack"
    DEQUEUE = "dequeue"
    TASK = "task"
    TASK_ACK = "task_ack"
    TASK_REQUEUE = "task_requeue"
    REPL_PREPARE = "repl_prepare"
    REPL_PREPARE_OK = "repl_prepare_ok"
    REPL_COMMIT = "repl_commit"
    REPL_ABORT = "repl_abort"
    ROUTE = "route"
    MAILBOX_PUSH = "mailbox_push"
    MAILBOX_DELIVER = "mailbox_deliver"
    SYNC_REQUEST = "sync_request"
    SYNC_STATE = "sync_state"


class Envelope(BaseModel):
    """Envelope padrão de mensagem."""

    type: MessageType
    source: int
    target: Optional[int] = None
    lamport_ts: int = Field(default=0)
    payload: Dict[str, Any] = Field(default_factory=dict)

    model_config = {
        "extra": "forbid",
    }
