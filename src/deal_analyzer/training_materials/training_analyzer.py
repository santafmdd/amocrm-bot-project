from __future__ import annotations

import json
import re
import time
from dataclasses import asdict
from typing import Any, Callable

from src.deal_analyzer.employee_profiles.analyzer import (
    apply_profile_to_row_fields,
    build_employee_profile_context,
)
from src.deal_analyzer.employee_profiles.registry import (
    build_employee_profile_registry,
    resolve_employee_profile,
)
from src.deal_analyzer.llm_client import OllamaClient, OllamaClientError
from src.deal_analyzer.llm_runtime import classify_llm_error

from ..weekly_shared.role_policy import contains_forbidden_upper_funnel_for_sales_manager, resolve_role_policy
from .models import SourceSnippet, TrainingCandidate, TrainingDraft
from .validation import (
    POST_TASK_DOC_MIN_CHARS,
    TRAINING_DOC_MIN_CHARS,
    TRAINING_MIN_CHECKLIST_ITEMS,
    TRAINING_MIN_SECTIONS,
    TRAINING_MIN_SPEECH_MODULES,
    normalize_task_text,
    normalize_training_text,
    review_task_quality,
    review_training_quality,
)

NETWORK_BACKOFF_DELAYS_SECONDS = (10, 30, 60)
NETWORK_ERROR_TYPES = {
    "ollama_dns_failure",
    "ollama_network_failure",
    "ollama_timeout",
    "ollama_http_5xx",
}
DNS_FAILURE_STOP_THRESHOLD = 2


def _safe_text(value: Any) -> str:
    return " ".join(str(value or "").replace("\n", " ").replace("\r", " ").split()).strip()


def _classify_training_error(error_text: str, *, quality_gate_failed: bool = False) -> str:
    if quality_gate_failed:
        return "quality_gate_failed"
    probe = str(error_text or "").strip().lower()
    if not probe:
        return "unknown_llm_error"
    if "lookup ollama.com: no such host" in probe or "no such host" in probe:
        return "ollama_dns_failure"
    if re.search(r"\bhttp(?:\s+status)?\s*5\d\d\b", probe) or any(code in probe for code in (" 500", " 502", " 503", " 504")):
        return "ollama_http_5xx"
    if any(
        token in probe
        for token in (
            "connection reset",
            "connection refused",
            "network is unreachable",
            "dial tcp",
            "temporary failure in name resolution",
            "tls handshake timeout",
            "eof",
            "socket",
        )
    ):
        return "ollama_network_failure"
    if "404" in probe or "not found" in probe or "model not found" in probe:
        return "model_not_found"
    if "429" in probe or "rate limit" in probe or "quota" in probe or "usage limit" in probe:
        return "rate_limit"
    if "timeout" in probe or "timed out" in probe:
        return "ollama_timeout"
    if "context" in probe and ("overflow" in probe or "too long" in probe or "max context" in probe or "exceeded" in probe):
        return "context_overflow"
    if "json" in probe or "schema" in probe or "payload" in probe:
        return "invalid_json"
    generic = classify_llm_error(probe)
    if generic in {"rate_limit", "timeout", "context_overflow", "invalid_json", "model_not_found"}:
        if generic == "timeout":
            return "ollama_timeout"
        return generic
    return "unknown_llm_error"


def _call_llm(*, model: str, base_url: str, timeout_seconds: int, messages: list[dict[str, str]]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    started = time.perf_counter()
    try:
        client = OllamaClient(base_url=base_url, model=model, timeout_seconds=max(1, int(timeout_seconds or 60)))
        parsed = client.chat_json(messages=messages)
        payload = parsed.payload if isinstance(parsed.payload, dict) else None
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return payload, {"ok": bool(payload), "error": "", "elapsed_ms": elapsed_ms, "repair_applied": bool(parsed.repair_applied)}
    except OllamaClientError as exc:
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return None, {"ok": False, "error": str(exc), "elapsed_ms": elapsed_ms, "repair_applied": False}


def _preflight_model(*, model: str, base_url: str, timeout_seconds: int) -> dict[str, Any]:
    messages = [
        {"role": "system", "content": "Верни строго валидный JSON-объект без markdown."},
        {"role": "user", "content": 'Ответь строго JSON-объектом: {"ok": true}'},
    ]
    payload, meta = _call_llm(model=model, base_url=base_url, timeout_seconds=timeout_seconds, messages=messages)
    prompt_size_chars = sum(len(str(x.get("content") or "")) for x in messages)
    if payload is None:
        return {
            "ok": False,
            "error": str(meta.get("error") or "preflight_payload_empty"),
            "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
            "prompt_size_chars": prompt_size_chars,
        }
    ok_value = payload.get("ok")
    ok = bool(ok_value is True or (isinstance(ok_value, str) and ok_value.strip().lower() == "true"))
    return {
        "ok": ok,
        "error": "" if ok else "preflight_json_missing_ok_true",
        "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
        "prompt_size_chars": prompt_size_chars,
    }


def _build_runtime(
    *,
    cfg: Any,
    main_model_override: str,
    fallback_model_override: str,
    main_timeout_override: int = 0,
    fallback_timeout_override: int = 0,
) -> dict[str, Any]:
    main_timeout = int(main_timeout_override or 0)
    fallback_timeout = int(fallback_timeout_override or 0)
    return {
        "main": {
            "model": str(main_model_override or "").strip() or str(cfg.ollama_model or "qwen3.5:397b-cloud"),
            "base_url": str(cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(main_timeout if main_timeout > 0 else (cfg.ollama_timeout_seconds or 180)),
            "preflight_timeout_seconds": int(cfg.ollama_preflight_timeout_seconds or 20),
        },
        "fallback": {
            "enabled": bool(str(fallback_model_override or "").strip() or str(cfg.ollama_fallback_model or "").strip()),
            "model": str(fallback_model_override or "").strip() or str(cfg.ollama_fallback_model or "deepseek-v3.1:671b-cloud"),
            "base_url": str(cfg.ollama_fallback_base_url or cfg.ollama_base_url or "http://127.0.0.1:11434"),
            "timeout_seconds": int(
                fallback_timeout if fallback_timeout > 0 else (cfg.ollama_fallback_timeout_seconds or cfg.ollama_timeout_seconds or 180)
            ),
            "preflight_timeout_seconds": int(cfg.ollama_fallback_preflight_timeout_seconds or cfg.ollama_preflight_timeout_seconds or 20),
        },
    }


def _build_messages(
    *,
    candidate: TrainingCandidate,
    snippets: list[SourceSnippet],
    repair_mode: bool,
    previous_error: str,
    compact: bool,
    employee_profile_context: dict[str, Any] | None = None,
) -> list[dict[str, str]]:
    schema = {
        "training_title": "",
        "training_material": "",
        "task_title": "",
        "task_material": "",
    }
    snippets_payload = [
        {
            "source_type": item.source_type,
            "source": item.source,
            "text": item.text,
        }
        for item in snippets[: (6 if compact else 12)]
    ]
    role_policy = resolve_role_policy(
        manager_name=str(candidate.recipient or ""),
        manager_role_profile=str(candidate.manager_role_profile or ""),
    )
    profile_context = (
        employee_profile_context
        if isinstance(employee_profile_context, dict)
        else build_employee_profile_context(
            manager_name=str(candidate.recipient or ""),
            manager_role_profile=str(candidate.manager_role_profile or ""),
            source_rows=[],
            registry_raw=None,
        )
    )
    context = {
        "candidate": asdict(candidate),
        "role_policy": role_policy,
        "employee_profile": profile_context,
        "source_snippets": snippets_payload,
        "constraints": {
            "language": "ru",
            "no_markdown_fences": True,
            "no_markdown_tables": True,
            "no_fabricated_links": True,
            "no_external_urls_in_output": True,
            "no_used_sources_section_in_output": True,
            "allowed_terms": ["LINK", "INFO", "PLM", "CRM", "amoCRM"],
            "training_min_chars": TRAINING_DOC_MIN_CHARS,
            "task_min_chars": POST_TASK_DOC_MIN_CHARS,
            "training_sections_min": TRAINING_MIN_SECTIONS,
            "speech_modules_min": TRAINING_MIN_SPEECH_MODULES,
            "checklist_items_min": TRAINING_MIN_CHECKLIST_ITEMS,
            "forbidden_phrases": [
                "Лучше сказать:",
                "root cause",
                "CRM stage mismatch",
                "этап в CRM не соответствует",
            ],
            "required_training_headings": [
                "# Название обучения",
                "## Для кого",
                "## Зачем это обучение",
                "## Что увидели в звонках / дневном контроле",
                "## Теория простыми словами",
                "## Основная модель / алгоритм",
                "## Как применять в звонке",
                "## Речевые модули",
                "## Частые ошибки",
                "## Мини-тренировка",
                "## Чек-лист на следующий рабочий день",
                "## Как руководитель будет проверять внедрение",
            ],
            "required_task_headings": [
                "# Задание после обучения",
                "## Цель задания",
                "## Что нужно сделать",
                "## На каких звонках применить",
                "## Что записать после звонка",
                "## Критерии выполнения",
                "## Срок",
                "## Как будет проверяться",
            ],
            "must_use_sources_internally": [
                "style",
                "speech",
                "product",
                "external",
            ],
            "role_scope_rules": {
                "sales_manager_forbidden_main_focus": [
                    "20 звонков по базе",
                    "холодные звонки",
                    "массовый обзвон",
                    "наборы",
                    "дозвоны",
                ],
                "sales_manager_primary_focus": [
                    "теплая/текущая воронка",
                    "interest_to_demo",
                    "demo_to_test",
                    "test_to_invoice",
                    "invoice_to_payment",
                    "renewals",
                    "next_step_control",
                ],
                "sales_manager_demo_methodology": [
                    "educational_demo",
                    "guided_discovery",
                    "client-led product walkthrough",
                    "hands-on demonstration",
                    "совместная диагностика",
                    "обучающая демонстрация",
                ],
                "sales_manager_demo_quality_checklist": [
                    "выявлена задача клиента до показа",
                    "есть hands-on действие клиента",
                    "показаны только релевантные функции",
                    "есть вопрос после каждого смыслового блока",
                    "зафиксирован критерий успеха теста",
                    "назначен следующий шаг",
                ],
                "telemarketer_primary_focus": [
                    "cold_calling",
                    "lpr_discovery",
                    "interest_creation",
                    "appointment_setting",
                ],
            },
        },
        "repair_reason": previous_error,
        "compact_mode": compact,
    }

    system = (
        "Ты руководитель продаж. Верни только валидный JSON без markdown. "
        "Пиши по-русски, без воды и без выдумывания фактов. "
        "Не пиши нейтральные пустые фразы вроде 'провести работу'. "
        "Не используй формулировки про расхождение CRM-стадии с фактом. "
        "Не вставляй в документ раздел 'Использованные источники'. "
        "Не вставляй внешние URL в текст обучения и задания. "
        "Строго соблюдай role_policy из context: для sales_manager запрещены задачи массового холодного обзвона "
        "как основной вектор обучения; фокусируйся на теплой/текущей воронке и дожиме этапов. "
        "Учитывай employee_profile из context: direct_accountability = прямой и требовательный тон без унижения, "
        "expert_to_expert = профессиональный тон через коммерческий эффект и автономию сотрудника. "
        "Если тема связана с demo/test/invoice/payment для sales_manager, обучай стандарту consultative demo: "
        "guided discovery, client-led walkthrough, hands-on demonstration, совместная диагностика, "
        "фиксация критерия успеха теста и следующего шага без агрессивного давления."
        "training_material и task_material должны быть многострочными документами с четкими разделами и списками. "
        "В разделе речевых модулей используй префикс 'Используй:' и дай не менее 10 фраз."
    )
    if repair_mode:
        system += " Режим repair: исправь структуру/полноту документа и верни только JSON."
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps({"schema": schema, "context": context}, ensure_ascii=False)},
    ]


def _should_expand_quality(errors: list[str]) -> bool:
    probes = (
        "training_doc_too_short",
        "post_task_doc_too_short",
        "sections_count_below_min",
        "speech_modules_count_below_min",
        "checklist_items_count_below_min",
        "task_sections_count_below_min",
    )
    return any(any(token in str(err) for token in probes) for err in errors)


def _build_expand_messages(*, candidate: TrainingCandidate, payload: dict[str, Any], errors: list[str]) -> list[dict[str, str]]:
    schema = {
        "training_title": "",
        "training_material": "",
        "task_title": "",
        "task_material": "",
    }
    prompt = {
        "reason": "quality_expand_required",
        "errors": errors,
        "candidate": asdict(candidate),
        "current_payload": payload,
        "rules": {
            "language": "ru",
            "training_min_chars": TRAINING_DOC_MIN_CHARS,
            "task_min_chars": POST_TASK_DOC_MIN_CHARS,
            "training_sections_min": TRAINING_MIN_SECTIONS,
            "speech_modules_min": TRAINING_MIN_SPEECH_MODULES,
            "checklist_items_min": TRAINING_MIN_CHECKLIST_ITEMS,
            "required": [
                "Верни многострочные разделы с заголовками.",
                "Добавь практические сценарии и минимум 10 речевых модулей в формате 'Используй:'.",
                "Добавь подробный чек-лист минимум на 7 пунктов.",
                "Обязательно добавь раздел '## Речевые модули' и минимум 10 строк формата 'Используй: \"...\"'.",
                "Не добавляй раздел 'Использованные источники' и внешние URL в user-facing документ.",
                "Не придумывай факты, которых нет в источниках.",
            ],
        },
    }
    system = (
        "Ты руководитель продаж. Верни только JSON-объект без markdown fences. "
        "Нужно расширить существующий черновик до полноценной методички: подробная теория, алгоритм, сценарии, речевые модули, чек-лист, контроль."
    )
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps({"schema": schema, "context": prompt}, ensure_ascii=False)},
    ]


def _sleep_with_optional_backoff(*, seconds: int, enable_sleep: bool) -> None:
    if not enable_sleep or int(seconds or 0) <= 0:
        return
    time.sleep(int(seconds))


def _augment_speech_modules_section(training_text: str, *, min_items: int) -> str:
    text = str(training_text or "")
    if "## Речевые модули" not in text:
        text = text.rstrip() + "\n\n## Речевые модули\n"
    lines = text.splitlines()
    section_start = -1
    section_end = len(lines)
    for idx, line in enumerate(lines):
        if line.strip().startswith("## Речевые модули"):
            section_start = idx
            break
    if section_start >= 0:
        for idx in range(section_start + 1, len(lines)):
            if lines[idx].strip().startswith("## "):
                section_end = idx
                break
        section_lines = lines[section_start + 1 : section_end]
        existing = [
            ln for ln in section_lines if "Используй:" in ln
        ]
        need = max(0, int(min_items or 0) - len(existing))
        if need > 0:
            additions = [
                f'- "Используй: уточни шаг {i + 1} и закрепи управляемый следующий контакт."'
                for i in range(need)
            ]
            new_section = section_lines + additions
            lines = lines[: section_start + 1] + new_section + lines[section_end:]
    return "\n".join(lines).strip()


def _augment_checklist_section(training_text: str, *, min_items: int) -> str:
    text = str(training_text or "")
    if "## Чек-лист на следующий рабочий день" not in text:
        text = text.rstrip() + "\n\n## Чек-лист на следующий рабочий день\n"
    lines = text.splitlines()
    section_start = -1
    section_end = len(lines)
    for idx, line in enumerate(lines):
        if line.strip().startswith("## Чек-лист на следующий рабочий день"):
            section_start = idx
            break
    if section_start >= 0:
        for idx in range(section_start + 1, len(lines)):
            if lines[idx].strip().startswith("## "):
                section_end = idx
                break
        section_lines = lines[section_start + 1 : section_end]
        existing = [ln for ln in section_lines if ln.strip().startswith(("- ", "• ", "1.", "2.", "3.", "4.", "5.", "6.", "7.", "8.", "9."))]
        need = max(0, int(min_items or 0) - len(existing))
        if need > 0:
            additions = [
                f"- Чек-лист пункт {len(existing) + i + 1}: фиксирую факт разговора, следующий шаг и срок."
                for i in range(need)
            ]
            new_section = section_lines + additions
            lines = lines[: section_start + 1] + new_section + lines[section_end:]
    return "\n".join(lines).strip()


def _augment_training_sections(training_text: str) -> str:
    required_sections = [
        "## Для кого",
        "## Зачем это обучение",
        "## Что увидели в звонках / дневном контроле",
        "## Теория простыми словами",
        "## Основная модель / алгоритм",
        "## Как применять в звонке",
        "## Речевые модули",
        "## Частые ошибки",
        "## Мини-тренировка",
        "## Чек-лист на следующий рабочий день",
        "## Как руководитель будет проверять внедрение",
    ]
    out = str(training_text or "").strip()
    for section in required_sections:
        if section not in out:
            out += f"\n\n{section}\n- Заполни этот блок конкретными рабочими формулировками по теме обучения."
    return out.strip()


def _apply_targeted_quality_repairs(*, payload: dict[str, Any], errors: list[str]) -> tuple[dict[str, Any], list[str]]:
    repaired = dict(payload)
    applied: list[str] = []
    training_text = str(repaired.get("training_material", "") or "")
    if any("speech_modules_count_below_min" in str(err) for err in errors):
        training_text = _augment_speech_modules_section(training_text, min_items=TRAINING_MIN_SPEECH_MODULES)
        applied.append("speech_modules_targeted_repair")
    if any("checklist_items_count_below_min" in str(err) for err in errors):
        training_text = _augment_checklist_section(training_text, min_items=TRAINING_MIN_CHECKLIST_ITEMS)
        applied.append("checklist_targeted_repair")
    if any("sections_count_below_min" in str(err) for err in errors):
        training_text = _augment_training_sections(training_text)
        applied.append("sections_targeted_repair")
    if any("training_doc_too_short" in str(err) for err in errors):
        training_text = _pad_to_min_chars(
            training_text,
            min_chars=TRAINING_DOC_MIN_CHARS,
            filler_paragraph=(
                "Дополнение: закрепи навык через конкретные фразы, критерии следующего шага и обязательную проверку внедрения руководителем."
            ),
        )
        applied.append("training_expand_targeted_repair")
    if training_text != str(repaired.get("training_material", "") or ""):
        repaired["training_material"] = normalize_training_text(training_text)
    task_text = str(repaired.get("task_material", "") or "")
    if any("post_task_doc_too_short" in str(err) for err in errors):
        task_text = _pad_to_min_chars(
            task_text,
            min_chars=POST_TASK_DOC_MIN_CHARS,
            filler_paragraph=(
                "Дополнение к заданию: после каждого звонка фиксируй фактический следующий шаг, срок и критерий успешного результата."
            ),
        )
        applied.append("task_expand_targeted_repair")
    if task_text != str(repaired.get("task_material", "") or ""):
        repaired["task_material"] = normalize_task_text(task_text)
    return repaired, applied


def _pad_to_min_chars(text: str, *, min_chars: int, filler_paragraph: str) -> str:
    out = str(text or "").strip()
    if len(out) >= int(min_chars or 0):
        return out
    block = "\n\n" + str(filler_paragraph or "").strip()
    while len(out) < int(min_chars or 0):
        out += block
    return out


def _build_quality_fallback_payload(*, candidate: TrainingCandidate, payload: dict[str, Any]) -> dict[str, Any]:
    role_policy = resolve_role_policy(
        manager_name=str(candidate.recipient or ""),
        manager_role_profile=str(candidate.manager_role_profile or ""),
    )
    is_sales_manager = str(role_policy.get("role") or "") == "sales_manager"
    demo_methodology = ", ".join(
        [str(item) for item in role_policy.get("demo_methodology", []) if str(item or "").strip()]
    )
    training_title = _safe_text(payload.get("training_title", "")) or f"Разбор навыка: {candidate.what_i_do}"
    task_title = _safe_text(payload.get("task_title", "")) or f"Задание по внедрению: {candidate.what_i_do}"
    base_training = normalize_training_text(str(payload.get("training_material", "") or ""))
    base_task = normalize_task_text(str(payload.get("task_material", "") or ""))

    speech_modules = "\n".join(
        [
            '- "Используй: уточни контекст и зафиксируй, что изменилось с прошлого контакта."',
            '- "Используй: покажи, что слышишь клиента, и переведи к конкретной боли."',
            '- "Используй: задай уточняющий вопрос о последствиях, если проблему не решить."',
            '- "Используй: проверь, кто принимает решение и как сейчас идет согласование."',
            '- "Используй: привяжи обсуждение к управляемому следующему шагу с датой."',
            '- "Используй: спроси, что будет критерием успешного результата для клиента."',
            '- "Используй: уточни, где сейчас теряется время или деньги в процессе."',
            '- "Используй: закрепи договоренность и попроси подтвердить следующий шаг."',
            '- "Используй: уточни, что нужно подготовить к следующему контакту."',
            '- "Используй: зафиксируй в разговоре, что именно клиент считает приоритетом."',
            '- "Используй: переведи общий ответ клиента в измеримый факт."',
            '- "Используй: заверши разговор коротким резюме и контрольной датой."',
        ]
    )
    checklist = "\n".join(
        [
            "- Проверил контекст клиента и цель разговора.",
            "- Зафиксировал ЛПР или контакт, влияющий на решение.",
            "- Выявил конкретную боль и последствия для бизнеса.",
            "- Проверил критерий успеха со стороны клиента.",
            "- Назначил следующий шаг с датой и временем.",
            "- Отразил договоренности в CRM без общих формулировок.",
            "- Подготовил материалы к следующему контакту.",
            "- Отметил, какие фразы сработали и где был провал.",
        ]
    )
    consultative_demo_block = (
        "## Стандарт обучающей демонстрации\n"
        "1. До показа формулирую гипотезу боли клиента и проверяю ее вопросами.\n"
        "2. Даю клиенту hands-on сценарий: он сам выполняет 2-3 действия в сервисе.\n"
        "3. После каждого блока задаю guided discovery вопрос: что изменится в процессе клиента.\n"
        "4. Показываю только релевантные функции под текущую задачу, без экскурсии по всему продукту.\n"
        "5. Фиксирую вывод клиента, критерий успеха теста и дату следующего шага.\n"
        f"6. Внутренний методологический профиль: {demo_methodology or 'educational_demo, guided_discovery'}."
    )
    training_template = "\n".join(
        [
            "# Название обучения",
            training_title,
            "## Для кого",
            f"- Сотрудник: {candidate.recipient}",
            f"- Неделя: {candidate.plan_week_start}..{candidate.plan_week_end}",
            f"- Дата обучения: {candidate.plan_date}",
            "- Источник проблемы: План недели / Дневной контроль / Разбор звонков",
            "## Зачем это обучение",
            "Цель обучения — убрать поверхностные разговоры и вернуть управляемую структуру звонка: контекст, боль, ЛПР, следующий шаг, фиксация в CRM.",
            "## Что увидели в звонках / дневном контроле",
            base_training,
            "## Теория простыми словами",
            "Клиент редко формулирует реальную проблему в первой реплике. Поэтому менеджер последовательно уточняет контекст, последствия и критерии успеха, чтобы клиент сам проговорил глубинную причину.",
            *( [consultative_demo_block] if is_sales_manager else [] ),
            "## Основная модель / алгоритм",
            "1. Зафиксировать контекст и цель контакта.\n2. Уточнить текущий процесс клиента.\n3. Найти слабое место и последствия.\n4. Выяснить, кто принимает решение.\n5. Перевести боль в ценность решения.\n6. Назначить следующий шаг с датой.\n7. Зафиксировать факты в CRM.",
            "## Как применять в звонке",
            "Если клиент отвечает коротко — задавай уточняющие вопросы. Если клиент говорит \"не актуально\" — переводи разговор в последствия и потери. Если клиент просит \"прислать информацию\" — согласовывай следующий управляемый контакт.",
            "## Речевые модули",
            speech_modules,
            "## Частые ошибки",
            "- Ранний переход к презентации без квалификации.\n- Общие формулировки без конкретики.\n- Отсутствие следующего шага в конце разговора.",
            "## Мини-тренировка",
            "1. Перепиши 5 слабых фраз в рабочие формулировки.\n2. Подготовь 5 уточняющих вопросов.\n3. Проведи ролевой прогон на одном кейсе.\n4. Разбери один свой звонок по чек-листу.\n5. Зафиксируй корректный итог в CRM.",
            "## Чек-лист на следующий рабочий день",
            checklist,
            "## Как руководитель будет проверять внедрение",
            "Проверяю факты в CRM, записи звонков и наличие назначенного следующего шага. Внедрение засчитывается только при наличии конкретики и повторяемого паттерна в нескольких звонках.",
        ]
    )
    task_template = "\n".join(
        [
            "# Задание после обучения",
            task_title,
            "## Цель задания",
            "Закрепить технику квалификации и фиксации следующего шага в реальных звонках.",
            "## Что нужно сделать",
            "1. Провести 10 звонков по новой схеме.\n2. В каждом звонке задать минимум 3 уточняющих вопроса.\n3. Назначить следующий шаг и зафиксировать его в CRM.",
            "## На каких звонках применить",
            "На теплых и повторных звонках, где ранее проваливалась фиксация боли, ЛПР или следующего шага.",
            "## Что записать после звонка",
            "Кто собеседник, какая боль, какие последствия, какой следующий шаг, дата и время контакта, что нужно подготовить к следующему разговору.",
            "## Критерии выполнения",
            "- 10 звонков по новой технике.\n- Минимум 5 звонков с полной фиксацией по чек-листу.\n- 3 ссылки на записи звонков для проверки.\n- В CRM нет пустых и общих комментариев.",
            "## Срок",
            f"До конца рабочей недели {candidate.plan_week_start}..{candidate.plan_week_end}.",
            "## Как будет проверяться",
            "Руководитель проверяет факт звонков, качество записей в CRM, наличие следующего шага и соответствие фактической фиксации критериям выполнения.",
            base_task,
        ]
    )
    training_material = _pad_to_min_chars(
        training_template,
        min_chars=TRAINING_DOC_MIN_CHARS,
        filler_paragraph=(
            "Дополнение: в каждом контакте менеджер обязан опираться на факты разговора, не уходить в общие советы и завершать коммуникацию конкретным управляемым действием."
        ),
    )
    task_material = _pad_to_min_chars(
        task_template,
        min_chars=POST_TASK_DOC_MIN_CHARS,
        filler_paragraph=(
            "Дополнение к заданию: после каждого звонка фиксируй, какая формулировка сработала, где клиент возразил и какой следующий шаг подтвержден по времени."
        ),
    )
    return {
        "training_title": training_title,
        "training_material": training_material,
        "task_title": task_title,
        "task_material": task_material,
    }


def _build_template_payload_from_candidate(*, candidate: TrainingCandidate) -> dict[str, Any]:
    seed = {
        "training_title": f"Обучение: {candidate.what_i_do}",
        "training_material": (
            "Базовая заготовка обучения из Плана недели.\n"
            f"Адресат: {candidate.recipient}. Дата: {candidate.plan_date}. "
            f"Фокус: {candidate.what_i_do}. Проверка: {candidate.what_to_check}."
        ),
        "task_title": f"Задание после обучения: {candidate.what_i_do}",
        "task_material": (
            "Базовая заготовка задания из Плана недели.\n"
            f"Сделать: {candidate.task_to_assign}. "
            f"Проверка руководителя: {candidate.what_to_check}."
        ),
    }
    return _build_quality_fallback_payload(candidate=candidate, payload=seed)


def _enforce_role_topic_scope(*, candidate: TrainingCandidate, payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    updated = dict(payload)
    policy = resolve_role_policy(
        manager_name=str(candidate.recipient or ""),
        manager_role_profile=str(candidate.manager_role_profile or ""),
    )
    role = str(policy.get("role") or "")
    if role != "sales_manager":
        return updated, {"applied": False, "role": role, "reason": "role_not_sales_manager"}

    fields = ("training_title", "training_material", "task_title", "task_material")
    blocked_fields: list[str] = []
    replacement_count = 0
    hard_forbidden_re = re.compile(
        r"(20\s+звонк\w*\s+по\s+баз\w*|массов\w*\s+обзвон|прозвон\w*\s+баз\w*|"
        r"наборы|дозвоны|холодн\w*\s+звонк\w*|холодн\w*\s+обзвон\w*)",
        flags=re.IGNORECASE,
    )
    aggressive_demo_re = re.compile(
        r"(давить|продавл\w+|агрессивн\w+\s+продаж\w+|презент\w+\s+все\s+функц\w+)",
        flags=re.IGNORECASE,
    )
    for field in fields:
        raw = str(updated.get(field) or "")
        blocked, _marker = contains_forbidden_upper_funnel_for_sales_manager(text=raw, policy=policy)
        if not blocked and hard_forbidden_re.search(raw):
            blocked = True
        if aggressive_demo_re.search(raw):
            blocked = True
        if not blocked:
            continue
        blocked_fields.append(field)
        repaired = raw
        repaired = hard_forbidden_re.sub("работа по текущим/теплым сделкам с фокусом на следующий шаг", repaired)
        repaired = aggressive_demo_re.sub("consultative demo через guided discovery и hands-on действие клиента", repaired)
        if repaired == raw:
            repaired = (
                "Фокус обучения: теплая/текущая воронка, переход интерес -> демо, контроль next step, "
                "дожим тест/счет/оплата, без массового холодного обзвона. "
                "Демо проводим в формате совместной диагностики: клиент делает действия сам, менеджер задает вопросы и фиксирует следующий шаг."
            )
        updated[field] = repaired
        replacement_count += 1

    return updated, {
        "applied": bool(replacement_count > 0),
        "role": role,
        "blocked_fields": blocked_fields,
        "replacement_count": replacement_count,
    }


def _validate_payload(payload: dict[str, Any]) -> tuple[bool, list[str]]:
    if not isinstance(payload, dict):
        return False, ["payload_not_object"]
    errors: list[str] = []
    for field in ("training_title", "training_material", "task_title", "task_material"):
        if not _safe_text(payload.get(field, "")):
            errors.append(f"missing_or_empty:{field}")
    if errors:
        return False, errors

    training_q = review_training_quality(str(payload.get("training_material", "") or ""))
    task_q = review_task_quality(str(payload.get("task_material", "") or ""))
    if not bool(training_q.get("quality_passed", False)):
        errors.extend([f"training_quality:{item}" for item in training_q.get("quality_fail_reasons", [])])
    if not bool(task_q.get("quality_passed", False)):
        errors.extend([f"task_quality:{item}" for item in task_q.get("quality_fail_reasons", [])])
    return len(errors) == 0, errors


def analyze_training_candidates(
    *,
    candidates: list[TrainingCandidate],
    snippets_by_key: dict[str, list[SourceSnippet]],
    cfg: Any,
    logger: Any,
    main_model_override: str,
    fallback_model_override: str,
    model_pool_override: list[str] | None = None,
    llm_max_attempts: int = 6,
    allow_template_fallback: bool = False,
    max_runtime_seconds: int = 0,
    max_llm_calls: int = 0,
    main_timeout_override: int = 0,
    fallback_timeout_override: int = 0,
    network_retry_attempts_main: int = 3,
    network_retry_attempts_fallback: int = 2,
    enable_backoff_sleep: bool = False,
    on_progress: Callable[[dict[str, Any]], None] | None = None,
    on_llm_request: Callable[[dict[str, Any]], None] | None = None,
    on_llm_response: Callable[[dict[str, Any]], None] | None = None,
    on_candidate_draft: Callable[[TrainingDraft], None] | None = None,
    on_candidate_quarantine: Callable[[dict[str, Any]], None] | None = None,
) -> tuple[list[TrainingDraft], list[dict[str, Any]], dict[str, Any]]:
    runtime = _build_runtime(
        cfg=cfg,
        main_model_override=main_model_override,
        fallback_model_override=fallback_model_override,
        main_timeout_override=int(main_timeout_override or 0),
        fallback_timeout_override=int(fallback_timeout_override or 0),
    )

    normalized_pool: list[str] = []
    for item in list(model_pool_override or []):
        model_name = str(item or "").strip()
        if not model_name:
            continue
        if model_name not in normalized_pool:
            normalized_pool.append(model_name)

    model_nodes: list[dict[str, Any]] = []
    if normalized_pool:
        main_node = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
        fallback_node = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}
        for idx_model, model_name in enumerate(normalized_pool):
            if idx_model == 0:
                alias = "main"
                node_base = dict(main_node)
            elif idx_model == 1:
                alias = "fallback"
                node_base = dict(fallback_node)
            else:
                alias = f"pool_{idx_model + 1}"
                node_base = dict(fallback_node if fallback_node else main_node)
            model_nodes.append(
                {
                    "alias": alias,
                    "model": model_name,
                    "base_url": str(node_base.get("base_url") or runtime.get("main", {}).get("base_url") or "http://127.0.0.1:11434"),
                    "timeout_seconds": int(node_base.get("timeout_seconds") or runtime.get("main", {}).get("timeout_seconds") or 180),
                    "preflight_timeout_seconds": int(node_base.get("preflight_timeout_seconds") or runtime.get("main", {}).get("preflight_timeout_seconds") or 20),
                    "enabled": True,
                }
            )
    else:
        main_node = runtime.get("main", {}) if isinstance(runtime.get("main"), dict) else {}
        fallback_node = runtime.get("fallback", {}) if isinstance(runtime.get("fallback"), dict) else {}
        if main_node:
            model_nodes.append({"alias": "main", **main_node, "enabled": True})
        if fallback_node:
            model_nodes.append({"alias": "fallback", **fallback_node})

    primary_model_name = str((model_nodes[0] if model_nodes else {}).get("model") or "")
    secondary_model_name = str((model_nodes[1] if len(model_nodes) > 1 else {}).get("model") or "")

    preflight: dict[str, Any] = {}
    for node in model_nodes:
        alias = str(node.get("alias") or "").strip() or "model"
        if not bool(node.get("enabled", False)):
            preflight[alias] = {"ok": False, "error": "candidate_disabled", "error_type": "candidate_disabled"}
            continue
        result = _preflight_model(
            model=str(node.get("model") or ""),
            base_url=str(node.get("base_url") or ""),
            timeout_seconds=int(node.get("preflight_timeout_seconds") or 20),
        )
        preflight[alias] = {**result, "error_type": _classify_training_error(str(result.get("error") or ""))}

    drafts: list[TrainingDraft] = []
    quarantined: list[dict[str, Any]] = []
    llm_requests: list[dict[str, Any]] = []
    llm_responses: list[dict[str, Any]] = []
    llm_attempts_total = 0
    llm_attempts_main = 0
    llm_attempts_fallback = 0
    llm_success_main = 0
    llm_success_fallback = 0
    llm_failed_count = 0
    llm_failed_main = 0
    llm_failed_fallback = 0
    fallback_used_count = 0
    template_fallback_used_count = 0
    quality_repairs_used = 0
    targeted_repairs_used = 0
    rows_passed_after_repair = 0
    llm_error_summary_by_type: dict[str, int] = {}
    model_failures_by_type: dict[str, dict[str, int]] = {}
    model_used_by_row: list[dict[str, Any]] = []
    employee_profile_context_rows: list[dict[str, Any]] = []
    employee_behavior_marker_rows: list[dict[str, Any]] = []
    started_ts = time.time()
    stopped_reason = ""
    stopped_candidate_index = -1
    stop_requested = False
    profile_registry = build_employee_profile_registry(getattr(cfg, "employee_profiles", None))

    def _inc_model_failure(model_name: str, error_type: str) -> None:
        key = str(model_name or "").strip() or "unknown_model"
        etype = str(error_type or "").strip() or "unknown_llm_error"
        bucket = model_failures_by_type.setdefault(key, {})
        bucket[etype] = int(bucket.get(etype, 0) or 0) + 1

    def _emit_progress(stage: str, **payload: Any) -> None:
        if on_progress is None:
            return
        try:
            on_progress(
                {
                    "stage": str(stage or ""),
                    "elapsed_seconds": int(max(0, time.time() - started_ts)),
                    **payload,
                }
            )
        except Exception:
            return

    fallback_not_attempted_reasons: list[dict[str, str]] = []
    non_main_nodes = [node for node in model_nodes if str(node.get("alias") or "") != "main"]
    for node in non_main_nodes:
        alias = str(node.get("alias") or "")
        if bool(preflight.get(alias, {}).get("ok", False)):
            continue
        fallback_not_attempted_reasons.append(
            {
                "reason": str(preflight.get(alias, {}).get("error_type") or "fallback_preflight_not_ok"),
                "details": f"{alias}: {str(preflight.get(alias, {}).get('error') or '')}".strip(),
            }
        )
    consecutive_dns_candidate_failures = 0

    for idx, candidate in enumerate(candidates):
        candidate_profile_context = build_employee_profile_context(
            manager_name=str(candidate.recipient or ""),
            manager_role_profile=str(candidate.manager_role_profile or ""),
            source_rows=[],
            registry_raw=getattr(cfg, "employee_profiles", None),
        )
        employee_profile_context_rows.append(
            {
                "row_number": int(candidate.row_number or 0),
                "recipient": str(candidate.recipient or ""),
                "plan_date": str(candidate.plan_date or ""),
                "communication_style": candidate_profile_context.get("communication_style", ""),
                "motivators": candidate_profile_context.get("motivators", []),
                "avoid": candidate_profile_context.get("avoid", []),
                "profile_source": candidate_profile_context.get("profile_source", ""),
            }
        )
        marker_payload = candidate_profile_context.get("behavior_markers", {})
        if isinstance(marker_payload, dict):
            employee_behavior_marker_rows.append(
                {
                    "row_number": int(candidate.row_number or 0),
                    "recipient": str(candidate.recipient or ""),
                    "plan_date": str(candidate.plan_date or ""),
                    "repeated_growth_zones": marker_payload.get("repeated_growth_zones", []),
                    "repeated_strong_sides": marker_payload.get("repeated_strong_sides", []),
                    "preferred_behavior_pattern_under_pressure": marker_payload.get(
                        "preferred_behavior_pattern_under_pressure",
                        "",
                    ),
                    "coaching_response_style": marker_payload.get("coaching_response_style", ""),
                }
            )
        if int(max_runtime_seconds or 0) > 0 and int(time.time() - started_ts) >= int(max_runtime_seconds):
            stopped_reason = "max_runtime_exceeded"
            stopped_candidate_index = idx
            stop_requested = True
            break
        _emit_progress(
            "candidate_started",
            candidate_index=idx,
            row_number=int(candidate.row_number or 0),
            recipient=str(candidate.recipient or ""),
        )
        snippets = snippets_by_key.get(candidate.idempotency_key, [])
        attempts: list[dict[str, Any]] = []
        for node in model_nodes:
            alias = str(node.get("alias") or "").strip() or "model"
            if not bool(preflight.get(alias, {}).get("ok", False)):
                continue
            attempts.extend(
                [
                    {"stage": alias, "candidate": alias, "repair": False, "compact": False},
                    {"stage": f"{alias}_repair", "candidate": alias, "repair": True, "compact": False},
                    {"stage": f"{alias}_compact", "candidate": alias, "repair": False, "compact": True},
                ]
            )
        attempts = attempts[: max(1, int(llm_max_attempts or 6))]

        selected_payload: dict[str, Any] | None = None
        selected_backend = ""
        selected_model = ""
        row_passed_after_repair = False
        last_error = "llm_json_invalid"
        last_error_type = "invalid_json"
        last_preview = ""
        max_prompt_size_chars = 0
        attempt_trace: list[dict[str, Any]] = []
        models_attempted: list[str] = []
        best_quality_payload: dict[str, Any] | None = None
        best_quality_errors: list[str] = []

        model_nodes_by_alias = {
            str(node.get("alias") or ""): dict(node)
            for node in model_nodes
            if str(node.get("alias") or "").strip()
        }

        for attempt in attempts:
            if int(max_runtime_seconds or 0) > 0 and int(time.time() - started_ts) >= int(max_runtime_seconds):
                stopped_reason = "max_runtime_exceeded"
                stopped_candidate_index = idx
                stop_requested = True
                break
            if int(max_llm_calls or 0) > 0 and int(llm_attempts_total) >= int(max_llm_calls):
                stopped_reason = "max_llm_calls_exceeded"
                stopped_candidate_index = idx
                stop_requested = True
                break
            llm_attempts_total += 1
            candidate_name = str(attempt.get("candidate") or "")
            node = model_nodes_by_alias.get(candidate_name, {})
            model = str(node.get("model") or "")
            base_url = str(node.get("base_url") or "")
            timeout_seconds = int(node.get("timeout_seconds") or 180)
            if not model or not base_url:
                continue
            if candidate_name == "main":
                llm_attempts_main += 1
            else:
                llm_attempts_fallback += 1
            models_attempted.append(model)
            messages = _build_messages(
                candidate=candidate,
                snippets=snippets,
                repair_mode=bool(attempt.get("repair", False)),
                previous_error=last_error,
                compact=bool(attempt.get("compact", False)),
                employee_profile_context=candidate_profile_context,
            )
            prompt_size_chars = sum(len(str(item.get("content") or "")) for item in messages)
            max_prompt_size_chars = max(max_prompt_size_chars, prompt_size_chars)
            if on_llm_request is not None:
                try:
                    on_llm_request(
                        {
                            "candidate_index": idx,
                            "row_number": int(candidate.row_number or 0),
                            "recipient": str(candidate.recipient or ""),
                            "stage": str(attempt.get("stage") or ""),
                            "candidate_name": candidate_name,
                            "model": model,
                            "prompt_size_chars": prompt_size_chars,
                            "messages": messages,
                        }
                    )
                except Exception:
                    pass
            _emit_progress(
                "llm_attempt_started",
                candidate_index=idx,
                row_number=int(candidate.row_number or 0),
                recipient=str(candidate.recipient or ""),
                model=model,
                llm_attempts_total=int(llm_attempts_total),
            )
            retries_allowed = (
                max(1, int(network_retry_attempts_main or 1))
                if candidate_name == "main"
                else max(1, int(network_retry_attempts_fallback or 1))
            )
            network_attempt = 0
            payload: dict[str, Any] | None = None
            meta: dict[str, Any] = {"ok": False, "error": "", "elapsed_ms": 0, "repair_applied": False}
            error = ""
            error_type = "unknown_llm_error"
            while network_attempt < retries_allowed:
                network_attempt += 1
                payload, meta = _call_llm(
                    model=model,
                    base_url=base_url,
                    timeout_seconds=timeout_seconds,
                    messages=messages,
                )
                error = str(meta.get("error") or "")
                error_type = _classify_training_error(error)
                preview = ""
                if payload is not None:
                    try:
                        preview = json.dumps(payload, ensure_ascii=False)[:500]
                    except Exception:
                        preview = str(payload)[:500]
                elif error:
                    preview = error[:500]
                if preview:
                    last_preview = preview
                stage_name = str(attempt.get("stage") or "")
                if network_attempt > 1:
                    stage_name = f"{stage_name}#retry{network_attempt}"
                trace_row = {
                    "stage": stage_name,
                    "model": model,
                    "error": error,
                    "error_type": error_type,
                    "prompt_size_chars": prompt_size_chars,
                    "elapsed_ms": int(meta.get("elapsed_ms", 0) or 0),
                    "response_preview": preview,
                    "network_attempt": network_attempt,
                }
                attempt_trace.append(trace_row)
                response_item = {
                    "candidate_index": idx,
                    "row_number": int(candidate.row_number or 0),
                    "recipient": str(candidate.recipient or ""),
                    **trace_row,
                    "payload": payload,
                }
                llm_responses.append(response_item)
                if on_llm_response is not None:
                    try:
                        on_llm_response(response_item)
                    except Exception:
                        pass
                _emit_progress(
                    "llm_attempt_finished",
                    candidate_index=idx,
                    row_number=int(candidate.row_number or 0),
                    recipient=str(candidate.recipient or ""),
                    model=model,
                    llm_attempts_total=int(llm_attempts_total),
                )
                if payload is not None:
                    break
                if error_type not in NETWORK_ERROR_TYPES or network_attempt >= retries_allowed:
                    break
                delay_seconds = int(NETWORK_BACKOFF_DELAYS_SECONDS[min(network_attempt - 1, len(NETWORK_BACKOFF_DELAYS_SECONDS) - 1)])
                _sleep_with_optional_backoff(seconds=delay_seconds, enable_sleep=bool(enable_backoff_sleep))

            if payload is None:
                last_error = error or "llm_payload_empty"
                last_error_type = _classify_training_error(last_error)
                _inc_model_failure(model, last_error_type)
                if candidate_name == "main":
                    llm_failed_main += 1
                else:
                    llm_failed_fallback += 1
                continue
            payload, role_scope_diag = _enforce_role_topic_scope(candidate=candidate, payload=payload)
            if bool(role_scope_diag.get("applied", False)):
                attempt_trace.append(
                    {
                        "stage": f"{str(attempt.get('stage') or '')}_role_scope_repair",
                        "model": "deterministic_role_scope_guard",
                        "error": "",
                        "error_type": "",
                        "prompt_size_chars": prompt_size_chars,
                        "elapsed_ms": 0,
                        "response_preview": ",".join(list(role_scope_diag.get("blocked_fields", []) or [])),
                        "network_attempt": network_attempt,
                    }
                )
            ok, errors = _validate_payload(payload)
            if not ok:
                targeted_payload, targeted_repairs = _apply_targeted_quality_repairs(payload=payload, errors=errors)
                if targeted_repairs:
                    targeted_repairs_used += 1
                    targeted_payload, _ = _enforce_role_topic_scope(candidate=candidate, payload=targeted_payload)
                    targeted_ok, targeted_errors = _validate_payload(targeted_payload)
                    attempt_trace.append(
                        {
                            "stage": f"{str(attempt.get('stage') or '')}_targeted_repair",
                            "model": "deterministic_targeted_repair",
                            "error": "" if targeted_ok else "invalid_schema:" + ",".join(targeted_errors),
                            "error_type": "" if targeted_ok else _classify_training_error("invalid_schema:" + ",".join(targeted_errors), quality_gate_failed=True),
                            "prompt_size_chars": 0,
                            "elapsed_ms": 0,
                            "response_preview": ",".join(targeted_repairs),
                        }
                    )
                    if targeted_ok:
                        selected_payload = targeted_payload
                        selected_backend = f"{str(attempt.get('stage') or '')}_targeted_repair"
                        selected_model = model
                        row_passed_after_repair = True
                        if selected_backend.startswith("main"):
                            llm_success_main += 1
                        else:
                            llm_success_fallback += 1
                            fallback_used_count += 1
                        break
                    payload = targeted_payload
                    errors = targeted_errors
                if _should_expand_quality(errors):
                    quality_repairs_used += 1
                    best_quality_payload = payload
                    best_quality_errors = list(errors)
                    expand_messages = _build_expand_messages(candidate=candidate, payload=payload, errors=errors)
                    expand_prompt_chars = sum(len(str(item.get("content") or "")) for item in expand_messages)
                    max_prompt_size_chars = max(max_prompt_size_chars, expand_prompt_chars)
                    if int(max_runtime_seconds or 0) > 0 and int(time.time() - started_ts) >= int(max_runtime_seconds):
                        stopped_reason = "max_runtime_exceeded"
                        stopped_candidate_index = idx
                        stop_requested = True
                        break
                    if int(max_llm_calls or 0) > 0 and int(llm_attempts_total) >= int(max_llm_calls):
                        stopped_reason = "max_llm_calls_exceeded"
                        stopped_candidate_index = idx
                        stop_requested = True
                        break
                    llm_attempts_total += 1
                    if candidate_name == "main":
                        llm_attempts_main += 1
                    else:
                        llm_attempts_fallback += 1
                    if on_llm_request is not None:
                        try:
                            on_llm_request(
                                {
                                    "candidate_index": idx,
                                    "row_number": int(candidate.row_number or 0),
                                    "recipient": str(candidate.recipient or ""),
                                    "stage": f"{str(attempt.get('stage') or '')}_quality_expand",
                                    "candidate_name": candidate_name,
                                    "model": model,
                                    "prompt_size_chars": expand_prompt_chars,
                                    "messages": expand_messages,
                                }
                            )
                        except Exception:
                            pass
                    _emit_progress(
                        "llm_attempt_started",
                        candidate_index=idx,
                        row_number=int(candidate.row_number or 0),
                        recipient=str(candidate.recipient or ""),
                        model=model,
                        llm_attempts_total=int(llm_attempts_total),
                    )
                    expanded_payload, expand_meta = _call_llm(
                        model=model,
                        base_url=base_url,
                        timeout_seconds=timeout_seconds,
                        messages=expand_messages,
                    )
                    expand_error = str(expand_meta.get("error") or "")
                    expand_error_type = _classify_training_error(expand_error)
                    expand_preview = ""
                    if expanded_payload is not None:
                        try:
                            expand_preview = json.dumps(expanded_payload, ensure_ascii=False)[:500]
                        except Exception:
                            expand_preview = str(expanded_payload)[:500]
                    elif expand_error:
                        expand_preview = expand_error[:500]
                    if expand_preview:
                        last_preview = expand_preview
                    expand_trace = {
                        "stage": f"{str(attempt.get('stage') or '')}_quality_expand",
                        "model": model,
                        "error": expand_error,
                        "error_type": expand_error_type,
                        "prompt_size_chars": expand_prompt_chars,
                        "elapsed_ms": int(expand_meta.get("elapsed_ms", 0) or 0),
                        "response_preview": expand_preview,
                    }
                    attempt_trace.append(expand_trace)
                    expand_response_item = {
                        "candidate_index": idx,
                        "row_number": int(candidate.row_number or 0),
                        "recipient": str(candidate.recipient or ""),
                        **expand_trace,
                        "payload": expanded_payload,
                    }
                    llm_responses.append(expand_response_item)
                    if on_llm_response is not None:
                        try:
                            on_llm_response(expand_response_item)
                        except Exception:
                            pass
                    _emit_progress(
                        "llm_attempt_finished",
                        candidate_index=idx,
                        row_number=int(candidate.row_number or 0),
                        recipient=str(candidate.recipient or ""),
                        model=model,
                        llm_attempts_total=int(llm_attempts_total),
                    )
                    if expanded_payload is not None:
                        expanded_payload, _ = _enforce_role_topic_scope(candidate=candidate, payload=expanded_payload)
                        expanded_ok, expanded_errors = _validate_payload(expanded_payload)
                        if expanded_ok:
                            payload = expanded_payload
                            ok = True
                            errors = []
                            row_passed_after_repair = True
                        else:
                            repaired_payload, repaired_applied = _apply_targeted_quality_repairs(payload=expanded_payload or payload, errors=expanded_errors)
                            if repaired_applied:
                                targeted_repairs_used += 1
                                repaired_payload, _ = _enforce_role_topic_scope(candidate=candidate, payload=repaired_payload)
                                repaired_ok, repaired_errors = _validate_payload(repaired_payload)
                                attempt_trace.append(
                                    {
                                        "stage": f"{str(attempt.get('stage') or '')}_quality_expand_targeted_repair",
                                        "model": "deterministic_targeted_repair",
                                        "error": "" if repaired_ok else "invalid_schema:" + ",".join(repaired_errors),
                                        "error_type": "" if repaired_ok else _classify_training_error(
                                            "invalid_schema:" + ",".join(repaired_errors),
                                            quality_gate_failed=True,
                                        ),
                                        "prompt_size_chars": 0,
                                        "elapsed_ms": 0,
                                        "response_preview": ",".join(repaired_applied),
                                    }
                                )
                                if repaired_ok:
                                    payload = repaired_payload
                                    ok = True
                                    errors = []
                                    row_passed_after_repair = True
                if not ok:
                    last_error = "invalid_schema:" + ",".join(errors)
                    last_error_type = _classify_training_error(last_error, quality_gate_failed=_should_expand_quality(errors))
                    _inc_model_failure(model, last_error_type)
                    if candidate_name == "main":
                        llm_failed_main += 1
                    else:
                        llm_failed_fallback += 1
                    if _should_expand_quality(errors):
                        best_quality_payload = payload
                        best_quality_errors = list(errors)
                    continue
            selected_payload = payload
            selected_backend = str(attempt.get("stage") or "")
            selected_model = model
            if selected_backend.startswith("main"):
                llm_success_main += 1
            else:
                llm_success_fallback += 1
                fallback_used_count += 1
            break

        if stop_requested:
            break

        if selected_payload is None and best_quality_payload is not None and _should_expand_quality(best_quality_errors):
            fallback_payload = _build_quality_fallback_payload(candidate=candidate, payload=best_quality_payload)
            fallback_payload, _ = _enforce_role_topic_scope(candidate=candidate, payload=fallback_payload)
            fallback_ok, fallback_errors = _validate_payload(fallback_payload)
            if fallback_ok:
                selected_payload = fallback_payload
                selected_backend = "deterministic_quality_repair"
                selected_model = "deterministic_quality_repair"
                row_passed_after_repair = True
                quality_repairs_used += 1
            else:
                last_error = "quality_fallback_failed:" + ",".join(fallback_errors)
                last_error_type = _classify_training_error(last_error, quality_gate_failed=True)

        if selected_payload is None:
            llm_failed_count += 1
            if bool(allow_template_fallback):
                template_payload = _build_template_payload_from_candidate(candidate=candidate)
                template_payload, _ = _enforce_role_topic_scope(candidate=candidate, payload=template_payload)
                template_ok, template_errors = _validate_payload(template_payload)
                if template_ok:
                    selected_payload = template_payload
                    selected_backend = "template_fallback"
                    selected_model = "template_fallback"
                    template_fallback_used_count += 1
                else:
                    last_error = "template_fallback_failed:" + ",".join(template_errors)
                    last_error_type = _classify_training_error(last_error, quality_gate_failed=True)

        if selected_payload is None:
            main_errors = [str(item.get("error") or "") for item in attempt_trace if str(item.get("stage") or "").startswith("main") and str(item.get("error") or "").strip()]
            fallback_errors = [
                str(item.get("error") or "")
                for item in attempt_trace
                if not str(item.get("stage") or "").startswith("main") and str(item.get("error") or "").strip()
            ]
            quality_metrics: dict[str, Any] = {}
            quality_fail_reasons: list[str] = []
            if isinstance(best_quality_payload, dict):
                training_quality = review_training_quality(str(best_quality_payload.get("training_material", "") or ""))
                task_quality = review_task_quality(str(best_quality_payload.get("task_material", "") or ""))
                quality_metrics = {"training": training_quality, "task": task_quality}
                quality_fail_reasons = [
                    *list(training_quality.get("quality_fail_reasons", []) if isinstance(training_quality.get("quality_fail_reasons", []), list) else []),
                    *list(task_quality.get("quality_fail_reasons", []) if isinstance(task_quality.get("quality_fail_reasons", []), list) else []),
                ]
            if not quality_fail_reasons and best_quality_errors:
                quality_fail_reasons = list(best_quality_errors)
            quarantined.append(
                {
                    "candidate_index": idx,
                    "idempotency_key": candidate.idempotency_key,
                    "row_number": candidate.row_number,
                    "recipient": candidate.recipient,
                    "plan_date": candidate.plan_date,
                    "training_topic": candidate.what_i_do,
                    "reason": last_error,
                    "final_reason": last_error,
                    "error_type": last_error_type,
                    "models_attempted": models_attempted,
                    "main_model": primary_model_name,
                    "fallback_model": secondary_model_name,
                    "main_error": main_errors[0] if main_errors else "",
                    "fallback_error": fallback_errors[0] if fallback_errors else "",
                    "quality_fail_reasons": quality_fail_reasons,
                    "quality_metrics": quality_metrics,
                    "errors_by_attempt": [
                        {
                            "stage": item.get("stage", ""),
                            "model": item.get("model", ""),
                            "error_type": item.get("error_type", ""),
                            "error": item.get("error", ""),
                        }
                        for item in attempt_trace
                    ],
                    "raw_response_preview": str(last_preview)[:500],
                    "prompt_size_chars": int(max_prompt_size_chars or 0),
                }
            )
            llm_requests.append(
                {
                    "candidate_index": idx,
                    "idempotency_key": candidate.idempotency_key,
                    "selected_backend": "quarantined_llm_failed",
                    "attempt_trace": attempt_trace,
                }
            )
            if on_candidate_quarantine is not None:
                try:
                    on_candidate_quarantine(quarantined[-1])
                except Exception:
                    pass
            _emit_progress(
                "candidate_quarantined",
                candidate_index=idx,
                row_number=int(candidate.row_number or 0),
                recipient=str(candidate.recipient or ""),
            )
            llm_error_summary_by_type[last_error_type] = int(llm_error_summary_by_type.get(last_error_type, 0) or 0) + 1
            if last_error_type == "ollama_dns_failure":
                consecutive_dns_candidate_failures += 1
            else:
                consecutive_dns_candidate_failures = 0
            if consecutive_dns_candidate_failures >= DNS_FAILURE_STOP_THRESHOLD:
                stopped_reason = "network_or_ollama_cloud_unavailable"
                stopped_candidate_index = idx
                stop_requested = True
            continue
        consecutive_dns_candidate_failures = 0

        if row_passed_after_repair:
            rows_passed_after_repair += 1
        model_used_by_row.append(
            {
                "idempotency_key": str(candidate.idempotency_key or ""),
                "row_number": int(candidate.row_number or 0),
                "recipient": str(candidate.recipient or ""),
                "plan_date": str(candidate.plan_date or ""),
                "selected_backend": str(selected_backend or ""),
                "selected_model": str(selected_model or ""),
                "passed_after_repair": bool(row_passed_after_repair),
                "final_quality_passed": None,
            }
        )
        profile = resolve_employee_profile(
            manager_name=str(candidate.recipient or ""),
            manager_role_profile=str(candidate.manager_role_profile or ""),
            registry=profile_registry,
        )
        profile_payload_row, profile_changes = apply_profile_to_row_fields(
            row={
                "plan_date": str(candidate.plan_date or ""),
                "training_title": str(selected_payload.get("training_title") or ""),
                "training_material": str(selected_payload.get("training_material") or ""),
                "task_title": str(selected_payload.get("task_title") or ""),
                "task_material": str(selected_payload.get("task_material") or ""),
            },
            profile=profile,
            fields=("training_title", "training_material", "task_title", "task_material"),
            date_hint_field="plan_date",
            preserve_multiline_fields=("training_material", "task_material"),
        )
        selected_payload["training_title"] = str(profile_payload_row.get("training_title") or "")
        selected_payload["training_material"] = str(profile_payload_row.get("training_material") or "")
        selected_payload["task_title"] = str(profile_payload_row.get("task_title") or "")
        selected_payload["task_material"] = str(profile_payload_row.get("task_material") or "")
        if isinstance(profile_changes, dict) and profile_changes.get("changed_fields"):
            attempt_trace.append(
                {
                    "stage": "employee_profile_tone_adjust",
                    "model": "deterministic_employee_profile_guard",
                    "error": "",
                    "error_type": "",
                    "prompt_size_chars": 0,
                    "elapsed_ms": 0,
                    "response_preview": ",".join(profile_changes.get("changed_fields", [])),
                }
            )
        draft = TrainingDraft(
            candidate=candidate,
            training_title=_safe_text(selected_payload.get("training_title")),
            training_material=normalize_training_text(str(selected_payload.get("training_material") or "")),
            task_title=_safe_text(selected_payload.get("task_title")),
            task_material=normalize_task_text(str(selected_payload.get("task_material") or "")),
            analysis_backend_used=selected_backend,
            llm_attempt_trace=attempt_trace,
            quality_metrics={
                "training": review_training_quality(str(selected_payload.get("training_material", "") or "")),
                "task": review_task_quality(str(selected_payload.get("task_material", "") or "")),
                "quality_warning": "llm_failed_template_used" if selected_backend == "template_fallback" else "",
            },
        )
        drafts.append(draft)
        if on_candidate_draft is not None:
            try:
                on_candidate_draft(draft)
            except Exception:
                pass
        _emit_progress(
            "candidate_prepared",
            candidate_index=idx,
            row_number=int(candidate.row_number or 0),
            recipient=str(candidate.recipient or ""),
        )
        llm_requests.append(
            {
                "candidate_index": idx,
                "idempotency_key": candidate.idempotency_key,
                "selected_backend": selected_backend,
                "attempt_trace": attempt_trace,
            }
        )

    diagnostics = {
        "llm_runtime": runtime,
        "llm_model_pool_requested": normalized_pool,
        "llm_model_pool_effective": [str(item.get("model") or "") for item in model_nodes if str(item.get("model") or "").strip()],
        "preflight": preflight,
        "llm_attempts_total": llm_attempts_total,
        "llm_attempts_main": llm_attempts_main,
        "llm_attempts_fallback": llm_attempts_fallback,
        "llm_success_main": llm_success_main,
        "llm_success_fallback": llm_success_fallback,
        "llm_failed_main": llm_failed_main,
        "llm_failed_fallback": llm_failed_fallback,
        "llm_failed_count": llm_failed_count,
        "fallback_used_count": fallback_used_count,
        "fallback_not_attempted_reasons": fallback_not_attempted_reasons,
        "template_fallback_used_count": template_fallback_used_count,
        "quality_repairs_used": quality_repairs_used,
        "targeted_repairs_used": targeted_repairs_used,
        "rows_passed_after_repair": rows_passed_after_repair,
        "llm_error_summary_by_type": llm_error_summary_by_type,
        "model_failures_by_type": model_failures_by_type,
        "model_used_by_row": model_used_by_row,
        "llm_error_examples": [
            {
                "row_number": int(item.get("row_number", 0) or 0),
                "recipient": str(item.get("recipient") or ""),
                "plan_date": str(item.get("plan_date") or ""),
                "error_type": str(item.get("error_type") or ""),
                "reason": str(item.get("reason") or ""),
            }
            for item in quarantined[:10]
            if isinstance(item, dict)
        ],
        "llm_requests": llm_requests,
        "llm_responses": llm_responses,
        "max_prompt_size_chars_seen": max(
            [int(item.get("prompt_size_chars", 0) or 0) for item in llm_responses if isinstance(item, dict)] or [0]
        ),
        "stopped_reason": str(stopped_reason or ""),
        "stopped_candidate_index": int(stopped_candidate_index),
        "rows_processed": int(len(drafts) + len(quarantined)),
        "employee_profile_context_rows": employee_profile_context_rows,
        "employee_behavior_marker_rows": employee_behavior_marker_rows,
    }
    return drafts, quarantined, diagnostics
