use std::path::Path;

const DISTANCE_DECAY: f64 = 0.7;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChunkStrategy {
    #[allow(dead_code)]
    FixedUtf8,
    Structure,
}

impl ChunkStrategy {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::FixedUtf8 => "fixed_utf8",
            Self::Structure => "structure",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ChunkSpan {
    pub(super) start: usize,
    pub(super) end: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ChunkPlan {
    pub(super) strategy: ChunkStrategy,
    pub(super) spans: Vec<ChunkSpan>,
}

#[derive(Debug, Clone, Copy)]
struct Breakpoint {
    position: usize,
    score: u16,
}

pub(super) fn plan_chunks(
    _path: &Path,
    content: &str,
    chunk_size: usize,
    overlap: usize,
) -> ChunkPlan {
    let break_window = chunk_size.saturating_div(2).max(1);
    ChunkPlan {
        strategy: ChunkStrategy::Structure,
        spans: structure_chunks(content, chunk_size, overlap, break_window),
    }
}

pub(super) fn fixed_utf8_chunks(
    content: &str,
    chunk_size: usize,
    overlap: usize,
) -> Vec<ChunkSpan> {
    if content.is_empty() || chunk_size == 0 {
        return Vec::new();
    }

    let len = content.len();
    let step = chunk_size.saturating_sub(overlap);
    if chunk_size >= len || step == 0 {
        return vec![ChunkSpan { start: 0, end: len }];
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < len {
        let desired_end = start.saturating_add(chunk_size).min(len);
        let mut end = floor_char_boundary(content, desired_end);
        if end <= start {
            end = ceil_char_boundary(content, start + 1);
        }

        chunks.push(ChunkSpan { start, end });
        if end >= len {
            break;
        }

        let desired_start = end.saturating_sub(overlap);
        let mut next_start = floor_char_boundary(content, desired_start);
        if next_start <= start {
            next_start = ceil_char_boundary(content, start + 1);
        }
        start = next_start;
    }

    chunks
}

pub(super) fn structure_chunks(
    content: &str,
    chunk_size: usize,
    overlap: usize,
    break_window: usize,
) -> Vec<ChunkSpan> {
    if content.is_empty() || chunk_size == 0 {
        return Vec::new();
    }

    let len = content.len();
    let step = chunk_size.saturating_sub(overlap);
    if chunk_size >= len || step == 0 {
        return vec![ChunkSpan { start: 0, end: len }];
    }

    let breakpoints = scan_breakpoints(content);
    if breakpoints.is_empty() {
        return fixed_utf8_chunks(content, chunk_size, overlap);
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < len {
        let target = start.saturating_add(chunk_size).min(len);
        let end = if target >= len {
            len
        } else {
            best_breakpoint(&breakpoints, start, target, break_window)
                .unwrap_or_else(|| fixed_end(content, start, chunk_size))
        };

        let end = if end <= start {
            fixed_end(content, start, chunk_size)
        } else {
            end
        };

        chunks.push(ChunkSpan { start, end });
        if end >= len {
            break;
        }

        let desired_start = end.saturating_sub(overlap);
        let mut next_start = floor_char_boundary(content, desired_start);
        if next_start <= start {
            next_start = ceil_char_boundary(content, start + 1);
        }
        start = next_start;
    }

    chunks
}

fn scan_breakpoints(content: &str) -> Vec<Breakpoint> {
    let mut breakpoints = Vec::new();
    let mut line_start = 0;
    let mut in_fence = false;

    while line_start < content.len() {
        let line_end = content[line_start..]
            .find('\n')
            .map(|offset| line_start + offset)
            .unwrap_or(content.len());
        let next_line_start = if line_end < content.len() {
            line_end + 1
        } else {
            content.len()
        };
        let line = &content[line_start..line_end];
        let trimmed = line.trim();
        let fence = is_code_fence(trimmed);

        if fence {
            push_breakpoint(&mut breakpoints, line_start, 80, content.len());
            in_fence = !in_fence;
            line_start = next_line_start;
            continue;
        }

        if !in_fence {
            if let Some(score) = heading_score(trimmed) {
                push_breakpoint(&mut breakpoints, line_start, score, content.len());
            } else if is_horizontal_rule(trimmed) {
                push_breakpoint(&mut breakpoints, line_start, 60, content.len());
            } else if trimmed.is_empty() {
                push_breakpoint(&mut breakpoints, next_line_start, 20, content.len());
            } else if is_list_item(trimmed) {
                push_breakpoint(&mut breakpoints, line_start, 5, content.len());
            }

            push_breakpoint(&mut breakpoints, next_line_start, 1, content.len());
        }

        line_start = next_line_start;
    }

    breakpoints
}

fn push_breakpoint(
    breakpoints: &mut Vec<Breakpoint>,
    position: usize,
    score: u16,
    content_len: usize,
) {
    if position == 0 || position >= content_len {
        return;
    }

    breakpoints.push(Breakpoint { position, score });
}

fn best_breakpoint(
    breakpoints: &[Breakpoint],
    start: usize,
    target: usize,
    break_window: usize,
) -> Option<usize> {
    let lower = target.saturating_sub(break_window);

    breakpoints
        .iter()
        .filter(|breakpoint| {
            breakpoint.position > start
                && breakpoint.position <= target
                && breakpoint.position >= lower
        })
        .copied()
        .max_by(|left, right| {
            let left_score = decayed_score(*left, target, break_window);
            let right_score = decayed_score(*right, target, break_window);
            left_score.total_cmp(&right_score)
        })
        .map(|breakpoint| breakpoint.position)
}

fn decayed_score(breakpoint: Breakpoint, target: usize, break_window: usize) -> f64 {
    let distance = target.saturating_sub(breakpoint.position);
    let normalized_distance = distance as f64 / break_window.max(1) as f64;
    f64::from(breakpoint.score) / (1.0 + (DISTANCE_DECAY * normalized_distance.powi(2)))
}

fn fixed_end(content: &str, start: usize, chunk_size: usize) -> usize {
    let desired_end = start.saturating_add(chunk_size).min(content.len());
    let end = floor_char_boundary(content, desired_end);
    if end > start {
        end
    } else {
        ceil_char_boundary(content, start + 1)
    }
}

fn floor_char_boundary(content: &str, index: usize) -> usize {
    let mut index = index.min(content.len());
    while index > 0 && !content.is_char_boundary(index) {
        index -= 1;
    }
    index
}

fn ceil_char_boundary(content: &str, index: usize) -> usize {
    let mut index = index.min(content.len());
    while index < content.len() && !content.is_char_boundary(index) {
        index += 1;
    }
    index
}

fn heading_score(trimmed: &str) -> Option<u16> {
    let level = trimmed
        .as_bytes()
        .iter()
        .take_while(|byte| **byte == b'#')
        .count();
    if !(1..=6).contains(&level) {
        return None;
    }

    if !matches!(trimmed.as_bytes().get(level), None | Some(b' ' | b'\t')) {
        return None;
    }

    Some(match level {
        1 => 100,
        2 => 90,
        3 => 80,
        4 => 70,
        5 => 60,
        6 => 50,
        _ => unreachable!("heading level range checked above"),
    })
}

fn is_code_fence(trimmed: &str) -> bool {
    trimmed.starts_with("```") || trimmed.starts_with("~~~")
}

fn is_horizontal_rule(trimmed: &str) -> bool {
    let bytes = trimmed.as_bytes();
    if bytes.len() < 3 {
        return false;
    }

    bytes.iter().all(|byte| *byte == b'-')
        || bytes.iter().all(|byte| *byte == b'*')
        || bytes.iter().all(|byte| *byte == b'_')
}

fn is_list_item(trimmed: &str) -> bool {
    trimmed.starts_with("- ")
        || trimmed.starts_with("* ")
        || trimmed.starts_with("+ ")
        || ordered_list_marker_len(trimmed).is_some()
}

fn ordered_list_marker_len(trimmed: &str) -> Option<usize> {
    let marker_end = trimmed.find(". ")?;
    if marker_end == 0 {
        return None;
    }

    trimmed[..marker_end]
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then_some(marker_end + 2)
}
