from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Callable
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import update_wrapper
from typing import Any, cast, overload

from lightpipe.models import CachePolicy, RetryPolicy


@dataclass(frozen=True, slots=True)
class ParameterRef[T]:
    name: str


@dataclass(frozen=True, slots=True)
class NodeRef[T]:
    node_id: str


@dataclass(frozen=True, slots=True)
class MappedRef[T]:
    node_id: str

    def collect(self) -> CollectedRef[list[T]]:
        return CollectedRef(self.node_id)


@dataclass(frozen=True, slots=True)
class CollectedRef[T]:
    node_id: str


type InputBinding = Any


@dataclass(frozen=True, slots=True)
class StageSpec:
    name: str
    function: Callable[..., Any]
    definition_hash: str
    retry: RetryPolicy
    timeout: float | None
    cache: CachePolicy | None


@dataclass(frozen=True, slots=True)
class NodeSpec:
    id: str
    stage: StageSpec
    args: tuple[InputBinding, ...]
    kwargs: dict[str, InputBinding]
    mapped: bool = False
    map_arg: int = 0

    @property
    def dependencies(self) -> set[str]:
        refs: set[str] = set()
        for value in (*self.args, *self.kwargs.values()):
            if isinstance(value, (NodeRef, MappedRef, CollectedRef)):
                refs.add(value.node_id)
        return refs


@dataclass(frozen=True, slots=True)
class GraphDefinition:
    name: str
    nodes: dict[str, NodeSpec]
    outputs: Any
    parameters: tuple[str, ...]
    definition_hash: str

    def public_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "definition_hash": self.definition_hash,
            "parameters": list(self.parameters),
            "nodes": [
                {
                    "id": node.id,
                    "stage": node.stage.name,
                    "stage_hash": node.stage.definition_hash,
                    "dependencies": sorted(node.dependencies),
                    "mapped": node.mapped,
                    "map_arg": node.map_arg,
                    "retry": {
                        "attempts": node.stage.retry.attempts,
                        "initial_delay": node.stage.retry.initial_delay,
                        "multiplier": node.stage.retry.multiplier,
                        "maximum_delay": node.stage.retry.maximum_delay,
                    },
                    "timeout": node.stage.timeout,
                    "cache": (
                        None
                        if node.stage.cache is None
                        else {
                            "ttl_seconds": node.stage.cache.ttl.total_seconds(),
                            "version": node.stage.cache.version,
                        }
                    ),
                    "args": _canonical_binding(node.args),
                    "kwargs": _canonical_binding(node.kwargs),
                }
                for node in self.nodes.values()
            ],
            "outputs": _canonical_binding(self.outputs),
        }


@dataclass(slots=True)
class _GraphBuilder:
    name: str
    nodes: dict[str, NodeSpec] = field(default_factory=dict)
    counter: int = 0

    def add(
        self,
        stage: StageSpec,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        *,
        mapped: bool,
    ) -> str:
        node_id = f"{stage.name}-{self.counter}"
        self.counter += 1
        self.nodes[node_id] = NodeSpec(node_id, stage, args, kwargs, mapped)
        return node_id


_builder: ContextVar[_GraphBuilder | None] = ContextVar("lightpipe_builder", default=None)


def _callable_hash(function: Callable[..., Any]) -> str:
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError):
        source = getattr(function, "__qualname__", repr(function))
    module = getattr(function, "__module__", "")
    qualified_name = getattr(function, "__qualname__", repr(function))
    identity = f"{module}:{qualified_name}:{source}"
    return hashlib.sha256(identity.encode()).hexdigest()


def _canonical_binding(value: Any) -> Any:
    if isinstance(value, ParameterRef):
        return {"parameter": value.name}
    if isinstance(value, NodeRef):
        return {"node": value.node_id}
    if isinstance(value, MappedRef):
        return {"mapped": value.node_id}
    if isinstance(value, CollectedRef):
        return {"collected": value.node_id}
    if isinstance(value, tuple):
        return [_canonical_binding(item) for item in value]
    if isinstance(value, list):
        return [_canonical_binding(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _canonical_binding(item) for key, item in value.items()}
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return {
        "literal_type": f"{type(value).__module__}.{type(value).__qualname__}",
        "repr": repr(value),
    }


class Stage[**P, R]:
    def __init__(
        self,
        function: Callable[P, R],
        *,
        name: str | None = None,
        retry: RetryPolicy | None = None,
        timeout: float | None = None,
        cache: CachePolicy | None = None,
    ) -> None:
        self.function = function
        self.spec = StageSpec(
            name or getattr(function, "__name__", type(function).__name__),
            function,
            _callable_hash(function),
            retry or RetryPolicy(),
            timeout,
            cache,
        )
        update_wrapper(self, function)

    def __call__(self, *args: Any, **kwargs: Any) -> R:
        builder = _builder.get()
        if builder is None:
            return self.function(*args, **kwargs)
        return cast(R, NodeRef(builder.add(self.spec, args, kwargs, mapped=False)))

    def map(self, collection: InputBinding, *args: Any, **kwargs: Any) -> MappedRef[R]:
        builder = _builder.get()
        if builder is None:
            raise RuntimeError("stage.map() may only be used while defining a pipeline")
        return MappedRef(builder.add(self.spec, (collection, *args), kwargs, mapped=True))


@overload
def stage[**P, R](
    function: Callable[P, R],
    /,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    timeout: float | None = None,
    cache: CachePolicy | None = None,
) -> Stage[P, R]: ...


@overload
def stage[**P, R](
    function: None = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    timeout: float | None = None,
    cache: CachePolicy | None = None,
) -> Callable[[Callable[P, R]], Stage[P, R]]: ...


def stage(
    function: Any = None,
    *,
    name: str | None = None,
    retries: int = 0,
    retry_delay: float = 0.0,
    timeout: float | None = None,
    cache: CachePolicy | None = None,
) -> Any:
    def decorate(target: Any) -> Any:
        return Stage(
            target,
            name=name,
            retry=RetryPolicy(attempts=retries + 1, initial_delay=retry_delay),
            timeout=timeout,
            cache=cache,
        )

    return decorate(function) if function is not None else decorate


class Pipeline[**P]:
    def __init__(self, function: Callable[P, Any], *, name: str | None = None) -> None:
        self.function = function
        self.name = name or getattr(function, "__name__", type(function).__name__)
        self._compiled: GraphDefinition | None = None
        update_wrapper(self, function)

    def compile(self) -> GraphDefinition:
        if self._compiled is not None:
            return self._compiled
        signature = inspect.signature(self.function)
        refs = {name: ParameterRef(name) for name in signature.parameters}
        builder = _GraphBuilder(self.name)
        token = _builder.set(builder)
        try:
            outputs = cast(Callable[..., Any], self.function)(**refs)
        finally:
            _builder.reset(token)
        canonical = {
            "name": self.name,
            "parameters": list(refs),
            "nodes": [
                {
                    "id": node.id,
                    "stage": node.stage.name,
                    "stage_hash": node.stage.definition_hash,
                    "dependencies": sorted(node.dependencies),
                    "mapped": node.mapped,
                    "args": _canonical_binding(node.args),
                    "kwargs": _canonical_binding(node.kwargs),
                }
                for node in builder.nodes.values()
            ],
        }
        canonical["outputs"] = _canonical_binding(outputs)
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()
        self._compiled = GraphDefinition(self.name, builder.nodes, outputs, tuple(refs), digest)
        return self._compiled

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> PipelineInvocation:
        bound = inspect.signature(self.function).bind(*args, **kwargs)
        bound.apply_defaults()
        return PipelineInvocation(self.compile(), dict(bound.arguments))


@dataclass(frozen=True, slots=True)
class PipelineInvocation:
    graph: GraphDefinition
    parameters: dict[str, Any]


@overload
def pipeline[**P](function: Callable[P, Any], /, *, name: str | None = None) -> Pipeline[P]: ...


@overload
def pipeline[**P](
    function: None = None, *, name: str | None = None
) -> Callable[[Callable[P, Any]], Pipeline[P]]: ...


def pipeline(function: Any = None, *, name: str | None = None) -> Any:
    def decorate(target: Any) -> Any:
        return Pipeline(target, name=name)

    return cast(Any, decorate(function) if function is not None else decorate)
