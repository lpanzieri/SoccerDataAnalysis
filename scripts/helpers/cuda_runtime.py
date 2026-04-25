from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ComputeBackendDecision:
    requested_backend: str
    selected_backend: str
    cuda_enabled: bool
    cupy_available: bool
    cuda_device_count: int
    allow_cuda_execution: bool
    reason: str


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def _detect_cupy_cuda_device_count() -> tuple[bool, int]:
    try:
        import cupy  # type: ignore
    except Exception:
        return False, 0

    try:
        device_count = int(cupy.cuda.runtime.getDeviceCount())
    except Exception:
        return True, 0

    return True, max(0, device_count)


def resolve_compute_backend(
    requested_backend: str = "auto",
    *,
    allow_cuda_execution: bool = False,
) -> ComputeBackendDecision:
    requested = (requested_backend or "auto").strip().lower()
    if requested not in {"auto", "cpu", "cuda"}:
        raise ValueError("requested_backend must be one of: auto, cpu, cuda")

    cuda_enabled = _env_bool("ENABLE_CUDA", True)
    cupy_available, cuda_device_count = _detect_cupy_cuda_device_count()
    cuda_runtime_available = cupy_available and cuda_device_count > 0

    if requested == "cpu":
        return ComputeBackendDecision(
            requested_backend=requested,
            selected_backend="cpu",
            cuda_enabled=cuda_enabled,
            cupy_available=cupy_available,
            cuda_device_count=cuda_device_count,
            allow_cuda_execution=allow_cuda_execution,
            reason="forced_cpu",
        )

    if not cuda_enabled:
        if requested == "cuda":
            raise RuntimeError("CUDA backend was requested but ENABLE_CUDA is disabled")
        return ComputeBackendDecision(
            requested_backend=requested,
            selected_backend="cpu",
            cuda_enabled=cuda_enabled,
            cupy_available=cupy_available,
            cuda_device_count=cuda_device_count,
            allow_cuda_execution=allow_cuda_execution,
            reason="cuda_disabled_by_env",
        )

    if requested == "cuda":
        if not cuda_runtime_available:
            raise RuntimeError(
                "CUDA backend was requested but no CUDA runtime/device is available. "
                "Install CuPy for your CUDA version and verify GPU visibility."
            )
        if allow_cuda_execution:
            return ComputeBackendDecision(
                requested_backend=requested,
                selected_backend="cuda",
                cuda_enabled=cuda_enabled,
                cupy_available=cupy_available,
                cuda_device_count=cuda_device_count,
                allow_cuda_execution=allow_cuda_execution,
                reason="forced_cuda",
            )
        return ComputeBackendDecision(
            requested_backend=requested,
            selected_backend="cpu",
            cuda_enabled=cuda_enabled,
            cupy_available=cupy_available,
            cuda_device_count=cuda_device_count,
            allow_cuda_execution=allow_cuda_execution,
            reason="phase1_cpu_only",
        )

    # requested == "auto"
    if cuda_runtime_available and allow_cuda_execution:
        return ComputeBackendDecision(
            requested_backend=requested,
            selected_backend="cuda",
            cuda_enabled=cuda_enabled,
            cupy_available=cupy_available,
            cuda_device_count=cuda_device_count,
            allow_cuda_execution=allow_cuda_execution,
            reason="auto_cuda",
        )

    if cuda_runtime_available and not allow_cuda_execution:
        reason = "cuda_detected_phase1_cpu_only"
    else:
        reason = "auto_cpu_fallback"

    return ComputeBackendDecision(
        requested_backend=requested,
        selected_backend="cpu",
        cuda_enabled=cuda_enabled,
        cupy_available=cupy_available,
        cuda_device_count=cuda_device_count,
        allow_cuda_execution=allow_cuda_execution,
        reason=reason,
    )
