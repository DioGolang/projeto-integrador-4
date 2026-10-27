"use client";

import { useEffect } from "react";

/**
 * Checks whether an error is a benign abort or cancellation signal
 * resulting from component unmount, order change, or stream termination.
 */
export function isAbortOrCanceled(err: unknown, signal?: AbortSignal): boolean {
  if (signal?.aborted) return true;
  if (!err) return false;

  if (typeof err === "object") {
    const anyErr = err as {
      name?: string;
      code?: unknown;
      message?: string;
      rawMessage?: string;
      cause?: unknown;
    };

    if (anyErr.name === "AbortError") return true;
    if (anyErr.code === 1 || anyErr.code === "canceled") return true;

    const msg = (anyErr.message || anyErr.rawMessage || String(err)).toLowerCase();
    if (
      msg.includes("aborted") ||
      msg.includes("[canceled]") ||
      msg.includes("the operation was aborted") ||
      msg.includes("this operation was aborted") ||
      msg.includes("abort error") ||
      msg.includes("user aborted a request") ||
      msg.includes("request was cancelled") ||
      msg.includes("signal is aborted without reason")
    ) {
      return true;
    }

    if (anyErr.cause && isAbortOrCanceled(anyErr.cause)) {
      return true;
    }
  }

  if (typeof err === "string") {
    const str = err.toLowerCase();
    return (
      str.includes("aborted") ||
      str.includes("[canceled]") ||
      str.includes("the operation was aborted") ||
      str.includes("this operation was aborted")
    );
  }

  return false;
}

export function GlobalErrorHandler() {
  useEffect(() => {
    function handleUnhandledRejection(event: PromiseRejectionEvent) {
      const reason = event.reason;
      if (isAbortOrCanceled(reason)) {
        event.preventDefault();
        if (typeof event.stopImmediatePropagation === "function") {
          event.stopImmediatePropagation();
        }
      }
    }

    function handleError(event: ErrorEvent) {
      if (isAbortOrCanceled(event.error) || isAbortOrCanceled(event.message)) {
        event.preventDefault();
        if (typeof event.stopImmediatePropagation === "function") {
          event.stopImmediatePropagation();
        }
      }
    }

    // Capture phase ensures we intercept the event before Turbopack's dev overlay
    window.addEventListener("unhandledrejection", handleUnhandledRejection, true);
    window.addEventListener("error", handleError, true);

    return () => {
      window.removeEventListener("unhandledrejection", handleUnhandledRejection, true);
      window.removeEventListener("error", handleError, true);
    };
  }, []);

  return null;
}
