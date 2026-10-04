import { computed, shallowRef } from 'vue'

export type TimeRangePreset = 'today' | '7d' | '30d'
export type TimeRange = TimeRangePreset | 'custom'

export type TimeRangeParams
  = | { period: TimeRangePreset, asOf: number }
    | { startDate: string, endDate: string }

export const timeRangePresets: { label: string, value: TimeRangePreset }[] = [
  { label: '今天', value: 'today' },
  { label: '最近 7 天', value: '7d' },
  { label: '最近 30 天', value: '30d' },
]

export const defaultTimeRange: TimeRangePreset = 'today'
// 自定义范围上限，日期选择器据此约束可选终点
export const maxCustomRangeDays = 366

export function useTimeRange(initialRange: TimeRangePreset = defaultTimeRange) {
  const timeRange = shallowRef<TimeRange>(initialRange)
  const customStartDate = shallowRef('')
  const customEndDate = shallowRef('')
  const appliedParams = shallowRef<TimeRangeParams>({ period: initialRange, asOf: Date.now() })
  const timeRangeParams = computed(() => appliedParams.value)

  // 预设与自定义互斥：选中预设时丢弃已应用的自定义日期，触发器回到「自定义」未应用态
  function selectPreset(range: TimeRangePreset) {
    timeRange.value = range
    customStartDate.value = ''
    customEndDate.value = ''
    appliedParams.value = { period: range, asOf: Date.now() }
  }

  // 起止由日期选择器保证合法：齐全、顺序正确且不超过 maxCustomRangeDays
  function selectCustomRange(startDate: string, endDate: string) {
    customStartDate.value = startDate
    customEndDate.value = endDate
    timeRange.value = 'custom'
    appliedParams.value = { startDate, endDate }
  }

  function clearCustomRange() {
    selectPreset(defaultTimeRange)
  }

  function latestTimeRangeParams(): TimeRangeParams {
    const current = appliedParams.value
    return 'period' in current
      ? { period: current.period, asOf: Date.now() }
      : current
  }

  return {
    timeRange,
    timeRangeParams,
    customStartDate,
    customEndDate,
    selectPreset,
    selectCustomRange,
    clearCustomRange,
    latestTimeRangeParams,
  }
}
