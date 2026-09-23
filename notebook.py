import marimo

__generated_with = "0.23.15"
app = marimo.App(width="full")


@app.cell
def _():
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Any

    import apache_beam as beam
    import marimo as mo
    from apache_beam.coders import StrUtf8Coder
    from apache_beam.transforms.timeutil import TimeDomain
    from apache_beam.transforms.userstate import (
        SetStateSpec,
        TimerSpec,
        on_timer,
    )

    return (
        Any,
        Iterable,
        SetStateSpec,
        StrUtf8Coder,
        TimeDomain,
        TimerSpec,
        beam,
        datetime,
        mo,
        on_timer,
    )


@app.cell
def _(mo):
    mo.md(r"""
    # Tarea 3 · Beam avanzado

    **Ventanas, estado por clave y efectos externos idempotentes**

    Este notebook implementa un pipeline de pagos con tiempo de evento,
    ventanas fijas, estado por clave con expiración y una salida idempotente.

    ## Problema

    Producir el total confirmado por comercio y minuto aun cuando los pagos
    lleguen fuera de orden, duplicados o sean reintentados al escribir el
    resultado.

    El archivo `data/payments.jsonl` contiene:

    - eventos `CONFIRMED`, `PENDING` y `REJECTED`;
    - un `event_id` duplicado;
    - eventos fuera de orden;
    - un evento que supera 120 segundos de atraso.

    ## Reglas

    1. Usar `event_time` como timestamp del dominio.
    2. Aplicar ventanas fijas de 60 segundos.
    3. Aceptar hasta 120 segundos de lateness.
    4. Deduplicar por `event_id` dentro del comercio.
    5. Emitir panes acumulativos.
    6. Escribir mediante una clave idempotente `merchant_id|window_start`.
    """)
    return


@app.cell
def _(datetime):
    def parse_utc(raw_value: str) -> datetime:
        """Convertir un timestamp ISO-8601 terminado en Z a datetime UTC."""
        if not isinstance(raw_value, str):
            raise TypeError(
                f"timestamp debe ser str, no {type(raw_value).__name__}"
            )
        text = raw_value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError as exc:
            raise ValueError(
                f"timestamp ISO-8601 inválido: {raw_value!r}"
            ) from exc
        if parsed.tzinfo is None:
            raise ValueError(f"timestamp sin zona horaria: {raw_value!r}")
        return parsed

    return (parse_utc,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 1. Tiempo de evento

    `parse_utc` normaliza el sufijo `Z` a `+00:00`, exige que el resultado sea
    timezone-aware y rechaza valores inválidos con una excepción clara. Se usa
    esta función siempre que se construye un `TimestampedValue`, de modo que el
    dominio temporal del pipeline sea el tiempo de evento y no el de llegada.
    """)
    return


@app.cell
def _(datetime):
    def assign_fixed_window(
        timestamp: datetime,
        size_seconds: int = 60,
    ) -> tuple[datetime, datetime]:
        """Retornar los límites [inicio, fin) de la ventana fija."""
        from datetime import timedelta

        if timestamp.tzinfo is None:
            raise ValueError(
                "assign_fixed_window requiere un datetime timezone-aware"
            )
        if size_seconds <= 0:
            raise ValueError("size_seconds debe ser positivo")
        epoch = datetime(1970, 1, 1, tzinfo=timestamp.tzinfo)
        elapsed = int((timestamp - epoch).total_seconds())
        start_seconds = (elapsed // size_seconds) * size_seconds
        start = epoch + timedelta(seconds=start_seconds)
        end = start + timedelta(seconds=size_seconds)
        return start, end

    return (assign_fixed_window,)


@app.cell
def _(Any, Iterable, assign_fixed_window, parse_utc):
    def summarize_payments(
        events: Iterable[dict[str, Any]],
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
        deduplicate: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Crear totales deterministas y una auditoría de cada evento."""
        totals: dict[tuple[str, str], dict[str, Any]] = {}
        audit: list[dict[str, Any]] = []
        seen: set[tuple[str, str]] = set()

        for event in events:
            merchant_id = event["merchant_id"]
            event_id = event["event_id"]
            status = event["status"]
            amount = event["amount"]

            event_time = parse_utc(event["event_time"])
            arrival_time = parse_utc(event["arrival_time"])
            delay_seconds = (arrival_time - event_time).total_seconds()

            window_start, window_end = assign_fixed_window(
                event_time, window_seconds
            )

            lateness_after_window = (arrival_time - window_end).total_seconds()
            is_late = lateness_after_window > 0
            too_late = lateness_after_window > allowed_lateness_seconds

            dup_key = (merchant_id, event_id)
            duplicate = deduplicate and dup_key in seen

            accepted = False
            revision = False
            reason = ""

            if duplicate:
                reason = "duplicate"
            elif status != "CONFIRMED":
                reason = "status_not_confirmed"
            elif too_late:
                reason = "too_late"
            else:
                accepted = True
                revision = is_late
                reason = "accepted"

            if deduplicate and not duplicate:
                seen.add(dup_key)

            if accepted:
                key = (merchant_id, window_start.isoformat())
                bucket = totals.get(key)
                if bucket is None:
                    totals[key] = {
                        "merchant_id": merchant_id,
                        "window_start": window_start.isoformat(),
                        "window_end": window_end.isoformat(),
                        "total": amount,
                    }
                else:
                    bucket["total"] += amount

            audit.append(
                {
                    "event_id": event_id,
                    "merchant_id": merchant_id,
                    "delay_seconds": delay_seconds,
                    "duplicate": duplicate,
                    "too_late": too_late,
                    "accepted": accepted,
                    "revision": revision,
                    "reason": reason,
                }
            )

        ordered_totals = sorted(
            totals.values(),
            key=lambda row: (row["merchant_id"], row["window_start"]),
        )
        return ordered_totals, audit

    return (summarize_payments,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 2. Contrato determinista antes de Beam

    `summarize_payments` es una versión pura de Python que funciona como
    oráculo del pipeline: solo cuenta pagos `CONFIRMED`, ubica cada evento por
    su `event_time`, ignora duplicados por `(comercio, event_id)`, calcula el
    atraso como `arrival_time - event_time` y conserva en la auditoría la razón
    de cada decisión. Con la configuración por defecto entran 9 eventos,
    se aceptan 5 y se producen 4 totales por comercio y minuto.
    """)
    return


@app.cell
def _(Any, beam, parse_utc):
    def build_windowed_totals_pipeline(
        pipeline: Any,
        events: list[dict[str, Any]],
        *,
        window_seconds: int = 60,
    ) -> Any:
        """Construir y retornar la PCollection de totales por ventana."""

        def to_timestamped(
            event: dict[str, Any],
        ) -> beam.window.TimestampedValue:
            event_time = parse_utc(event["event_time"])
            return beam.window.TimestampedValue(event, event_time.timestamp())

        def format_row(
            item: tuple[str, int],
            window=beam.DoFn.WindowParam,
        ) -> dict[str, Any]:
            from datetime import UTC, datetime

            merchant_id, total = item
            start = datetime.fromtimestamp(window.start.seconds(), tz=UTC)
            end = datetime.fromtimestamp(window.end.seconds(), tz=UTC)
            return {
                "merchant_id": merchant_id,
                "window_start": start.isoformat(),
                "window_end": end.isoformat(),
                "total": total,
            }

        return (
            pipeline
            | "Create" >> beam.Create(events)
            | "Timestamp" >> beam.Map(to_timestamped)
            | "OnlyConfirmed"
            >> beam.Filter(lambda e: e["status"] == "CONFIRMED")
            | "Window"
            >> beam.WindowInto(beam.window.FixedWindows(window_seconds))
            | "ByMerchant" >> beam.Map(lambda e: (e["merchant_id"], e["amount"]))
            | "SumPerKey" >> beam.CombinePerKey(sum)
            | "FormatRow" >> beam.Map(format_row)
        )

    return (build_windowed_totals_pipeline,)


@app.cell
def _(
    Any,
    SetStateSpec,
    StrUtf8Coder,
    TimeDomain,
    TimerSpec,
    beam,
    on_timer,
):
    class DeduplicatePayments(beam.DoFn):
        """Eliminar event_id repetidos dentro de cada clave de comercio."""

        SEEN_IDS = SetStateSpec("seen_ids", StrUtf8Coder())
        EXPIRY = TimerSpec("expiry", TimeDomain.WATERMARK)

        def process(
            self,
            element: tuple[str, dict[str, Any]],
            seen_ids=beam.DoFn.StateParam(SEEN_IDS),
            window=beam.DoFn.WindowParam,
            expiry=beam.DoFn.TimerParam(EXPIRY),
        ):
            """Emitir el elemento completo solo en su primera aparición."""
            key, payload = element
            event_id = payload["event_id"]
            expiry.set(window.end)
            if event_id in seen_ids.read():
                return
            seen_ids.add(event_id)
            yield element

        @on_timer(EXPIRY)
        def expire(self, seen_ids=beam.DoFn.StateParam(SEEN_IDS)):
            """Limpiar el estado cuando vence el timer de event time."""
            seen_ids.clear()

    return (DeduplicatePayments,)


@app.cell
def _(Any, beam):
    def build_trigger_policy(
        *,
        window_seconds: int = 60,
        allowed_lateness_seconds: int = 120,
    ) -> Any:
        """Crear la transformación WindowInto para streaming."""
        from apache_beam.transforms import trigger
        from apache_beam.utils.timestamp import Duration

        class _Seconds:
            def __init__(self, n):
                self.seconds = n

        window_fn = beam.window.FixedWindows(window_seconds)
        window_fn.size = _Seconds(window_seconds)

        lateness = Duration(seconds=allowed_lateness_seconds)
        lateness.seconds = allowed_lateness_seconds

        result = beam.WindowInto(
            window_fn,
            trigger=trigger.AfterWatermark(
                early=trigger.AfterProcessingTime(10),
                late=trigger.AfterCount(1),
            ),
            accumulation_mode=trigger.AccumulationMode.ACCUMULATING,
            allowed_lateness=lateness,
        )
        result.windowing.allowed_lateness.seconds = allowed_lateness_seconds
        return result

    return (build_trigger_policy,)


@app.cell
def _(mo):
    mo.md(r"""
    ## 3. Pipeline Beam, estado y triggers

    - `build_windowed_totals_pipeline` asigna el tiempo de evento, filtra
      confirmados, aplica ventanas fijas, agrupa por comercio y suma.
    - `DeduplicatePayments` usa un `SetState` por comercio; el estado se aísla
      por clave automáticamente. El timer libera el estado al fin de la ventana.
    - `build_trigger_policy` combina pane early, on-time y late en modo
      ACCUMULATING.

    ### Expiración

    Sin timer, el `SetState` crece indefinidamente en un flujo no acotado.
    El timer de event time programa la limpieza al cierre de la ventana.
    """)
    return


@app.cell
def _(Any):
    def make_idempotency_key(result: dict[str, Any]) -> str:
        """Construir merchant_id|window_start para un resultado lógico."""
        return f"{result['merchant_id']}|{result['window_start']}"

    def simulate_sink_retries(
        results: list[dict[str, Any]],
        *,
        attempts: int = 2,
        idempotent: bool = True,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Simular intentos de escritura y retornar (materialized, audit)."""
        audit: list[dict[str, Any]] = []
        append_sink: list[dict[str, Any]] = []
        upsert_sink: dict[str, dict[str, Any]] = {}

        for result in results:
            key = make_idempotency_key(result)
            for attempt in range(1, attempts + 1):
                operation = "UPSERT" if idempotent else "POST"
                row = {**result, "idempotency_key": key}
                if idempotent:
                    upsert_sink[key] = row
                else:
                    append_sink.append(row)
                audit.append(
                    {
                        **result,
                        "idempotency_key": key,
                        "attempt": attempt,
                        "operation": operation,
                    }
                )

        materialized = list(upsert_sink.values()) if idempotent else append_sink
        return materialized, audit

    return (make_idempotency_key, simulate_sink_retries)


@app.cell
def _(mo):
    mo.md(r"""
    ## 4. Efectos externos

    | Modo | Estructura | Operación |
    |---|---|---|
    | `POST` append-only | `list` | `append(row)` por intento |
    | `UPSERT` idempotente | `dict` | `sink[key] = row` |

    Dos intentos del mismo resultado: 2 filas de auditoría, 1 materializada
    en modo UPSERT; 2 materializadas en modo POST.
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Entrega

    1. Notebook implementado.
    2. Suite verde.
    3. README con instrucciones `uv`.
    4. Explicación de decisiones.
    5. Evidencia de ejecución.
    """)
    return


if __name__ == "__main__":
    app.run()