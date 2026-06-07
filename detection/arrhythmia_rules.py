"""
Rule-Based Arrhythmia Detection
================================
Implements clinical decision rules for ECG arrhythmia detection.
Designed for explainability and EU MDR compliance — rules are transparent,
auditable, and referenced against established clinical thresholds.

Clinical references:
  - Normal resting heart rate: 60–100 bpm (AHA/ESC guidelines)
  - Bradycardia: sustained HR < 60 bpm
  - Tachycardia: sustained HR > 100 bpm
  - AFib proxy: high RR interval variability (SDNN > 50 ms)
  - Annotation-based: MIT-BIH beat labels used as ground truth

EU MDR 2017/745 note:
  All rules are explicitly defined with thresholds and clinical rationale.
  No black-box logic. Each alert carries a triggered_rule field for audit.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ArrhythmiaType(str, Enum):
    NORMAL                  = "Normal"
    BRADYCARDIA             = "Bradycardia"
    TACHYCARDIA             = "Tachycardia"
    POSSIBLE_AFIB           = "Possible_AFib"
    VENTRICULAR_PREMATURE   = "Ventricular_Premature_Contraction"
    ATRIAL_PREMATURE        = "Atrial_Premature_Beat"
    SIGNAL_QUALITY          = "Poor_Signal_Quality"


class Severity(str, Enum):
    LOW      = "Low"
    MEDIUM   = "Medium"
    HIGH     = "High"
    CRITICAL = "Critical"


@dataclass
class DetectionResult:
    arrhythmia_type:    ArrhythmiaType
    severity:           Severity
    triggered_rule:     str
    heart_rate_bpm:     Optional[float]
    rr_variability_ms:  Optional[float]
    confidence:         float        # 0.0 to 1.0
    requires_alert:     bool


# ---------------------------------------------------------------------------
# Clinical thresholds — documented for MDR audit trail
# ---------------------------------------------------------------------------
BRADYCARDIA_THRESHOLD_BPM       = 60.0
TACHYCARDIA_THRESHOLD_BPM       = 100.0
SEVERE_BRADYCARDIA_BPM          = 40.0
SEVERE_TACHYCARDIA_BPM          = 150.0
AFIB_SDNN_THRESHOLD_MS          = 50.0   # SDNN >50ms = irregular RR intervals
HIGH_AFIB_SDNN_THRESHOLD_MS     = 100.0  # Higher confidence possible AFib
MINIMUM_BEATS_FOR_ANALYSIS      = 3      # Minimum beats for reliable HR estimate

# MIT-BIH annotation symbols that represent arrhythmias
VENTRICULAR_ANNOTATIONS = {'V', 'E', 'F'}   # Ventricular ectopics
ATRIAL_ANNOTATIONS      = {'A', 'a', 'J', 'S', 'j'}  # Supraventricular ectopics


def detect_from_window(
    heart_rate_bpm:             float,
    rr_std_ms:                  float,
    beat_count:                 int,
    arrhythmia_annotation_count: int,
    arrhythmia_annotation_types: str,
) -> DetectionResult:
    """
    Apply clinical decision rules to window-level aggregated ECG metrics.

    Rules are applied in priority order — annotation-based rules (highest
    confidence, ground truth) fire before rate-based rules.

    Args:
        heart_rate_bpm:              Estimated mean HR over the window (bpm).
        rr_std_ms:                   Std deviation of RR intervals (ms) — proxy for
                                     HRV; high values suggest irregular rhythm.
        beat_count:                  Total beats detected in window.
        arrhythmia_annotation_count: Non-normal beat count per MIT-BIH labels.
        arrhythmia_annotation_types: Comma-separated annotation symbols in window.

    Returns:
        DetectionResult with classification, severity, and alert flag.
    """

    # Guard: insufficient data for analysis
    if beat_count < MINIMUM_BEATS_FOR_ANALYSIS:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.SIGNAL_QUALITY,
            severity           = Severity.LOW,
            triggered_rule     = "insufficient_beats_for_analysis",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.3,
            requires_alert     = False,
        )

    ann_types = set(arrhythmia_annotation_types.split(',')) \
        if arrhythmia_annotation_types else set()

    # ------------------------------------------------------------------
    # Rule 1: Ventricular ectopic beats (MIT-BIH annotation — ground truth)
    # V = Premature Ventricular Contraction, E = Ventricular escape,
    # F = Fusion of ventricular and normal beat
    # Clinical priority: HIGH — ventricular arrhythmias can deteriorate rapidly
    # ------------------------------------------------------------------
    if ann_types & VENTRICULAR_ANNOTATIONS:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.VENTRICULAR_PREMATURE,
            severity           = Severity.HIGH,
            triggered_rule     = "annotation_ventricular_ectopic_V_E_F",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.95,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 2: Atrial/supraventricular ectopic beats (MIT-BIH annotation)
    # A = Atrial premature, a = Aberrated atrial premature,
    # J = Nodal premature, S = Supraventricular premature
    # ------------------------------------------------------------------
    if ann_types & ATRIAL_ANNOTATIONS:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.ATRIAL_PREMATURE,
            severity           = Severity.MEDIUM,
            triggered_rule     = "annotation_atrial_ectopic_A_a_J_S",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.90,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 3: Severe bradycardia — possible cardiac arrest risk
    # Threshold: <40 bpm sustained (AHA ACLS guidelines)
    # ------------------------------------------------------------------
    if heart_rate_bpm < SEVERE_BRADYCARDIA_BPM:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.BRADYCARDIA,
            severity           = Severity.CRITICAL,
            triggered_rule     = f"hr_below_{SEVERE_BRADYCARDIA_BPM}_bpm_severe",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.85,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 4: Bradycardia
    # Threshold: <60 bpm (standard AHA/ESC definition)
    # ------------------------------------------------------------------
    if heart_rate_bpm < BRADYCARDIA_THRESHOLD_BPM:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.BRADYCARDIA,
            severity           = Severity.MEDIUM,
            triggered_rule     = f"hr_below_{BRADYCARDIA_THRESHOLD_BPM}_bpm",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.80,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 5: Severe tachycardia
    # Threshold: >150 bpm (risk of haemodynamic compromise)
    # ------------------------------------------------------------------
    if heart_rate_bpm > SEVERE_TACHYCARDIA_BPM:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.TACHYCARDIA,
            severity           = Severity.CRITICAL,
            triggered_rule     = f"hr_above_{SEVERE_TACHYCARDIA_BPM}_bpm_severe",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.85,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 6: Tachycardia
    # Threshold: >100 bpm (standard AHA/ESC definition)
    # ------------------------------------------------------------------
    if heart_rate_bpm > TACHYCARDIA_THRESHOLD_BPM:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.TACHYCARDIA,
            severity           = Severity.MEDIUM,
            triggered_rule     = f"hr_above_{TACHYCARDIA_THRESHOLD_BPM}_bpm",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.80,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 7: Possible AFib — high RR variability with normal rate
    # SDNN proxy: signal std deviation scaled to ms
    # High confidence threshold: SDNN > 100ms
    # ------------------------------------------------------------------
    if rr_std_ms > HIGH_AFIB_SDNN_THRESHOLD_MS:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.POSSIBLE_AFIB,
            severity           = Severity.HIGH,
            triggered_rule     = f"rr_sdnn_above_{HIGH_AFIB_SDNN_THRESHOLD_MS}ms_high",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.65,
            requires_alert     = True,
        )

    # ------------------------------------------------------------------
    # Rule 8: Possible AFib — moderate RR variability
    # Flagged for monitoring but not immediate alert
    # ------------------------------------------------------------------
    if rr_std_ms > AFIB_SDNN_THRESHOLD_MS:
        return DetectionResult(
            arrhythmia_type    = ArrhythmiaType.POSSIBLE_AFIB,
            severity           = Severity.MEDIUM,
            triggered_rule     = f"rr_sdnn_above_{AFIB_SDNN_THRESHOLD_MS}ms",
            heart_rate_bpm     = heart_rate_bpm,
            rr_variability_ms  = rr_std_ms,
            confidence         = 0.55,
            requires_alert     = False,
        )

    # ------------------------------------------------------------------
    # Default: Normal sinus rhythm
    # All thresholds within acceptable clinical range
    # ------------------------------------------------------------------
    return DetectionResult(
        arrhythmia_type    = ArrhythmiaType.NORMAL,
        severity           = Severity.LOW,
        triggered_rule     = "all_thresholds_within_normal_range",
        heart_rate_bpm     = heart_rate_bpm,
        rr_variability_ms  = rr_std_ms,
        confidence         = 0.90,
        requires_alert     = False,
    )
