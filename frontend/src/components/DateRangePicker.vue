<script setup lang="ts">
import { BaseIconButton, BasePopover } from '@codex-proxy/ui'
import { Calendar, ChevronDown, ChevronLeft, ChevronRight, X } from '@lucide/vue'
import { useEventListener, useMediaQuery } from '@vueuse/core'
import { computed, nextTick, shallowRef, useTemplateRef, watch } from 'vue'

interface DayCell {
  key: string
  day: number
  inViewMonth: boolean
  isToday: boolean
  isStart: boolean
  isEnd: boolean
  inRange: boolean
  disabled: boolean
  /** 双月并排除重后被隐藏的邻月日期：只占位，不渲染按钮 */
  placeholder: boolean
}

defineOptions({ inheritAttrs: false })

const props = withDefaults(
  defineProps<{
    /** 已提交的自定义起止日期（yyyy-mm-dd），为空表示未应用自定义范围 */
    start?: string
    end?: string
    /** 自定义范围最大天数，超出终点在日历中禁用 */
    maxRangeDays?: number
    disabled?: boolean
    ariaLabel?: string
  }>(),
  {
    start: '',
    end: '',
    maxRangeDays: 366,
    disabled: false,
    ariaLabel: '自定义时间范围',
  },
)

const emit = defineEmits<{
  custom: [startDate: string, endDate: string]
  clear: []
}>()

const open = shallowRef(false)
const triggerRef = useTemplateRef<HTMLButtonElement>('triggerRef')
const panelRef = useTemplateRef<HTMLElement>('panelRef')

// 与模板断点一致：宽屏双月并排，窄屏单月
const wideViewport = useMediaQuery('(min-width: 560px)')
const visibleMonths = computed(() => (wideViewport.value ? 2 : 1))

function pad2(value: number) {
  return String(value).padStart(2, '0')
}

function dateKey(date: Date) {
  return `${date.getFullYear()}-${pad2(date.getMonth() + 1)}-${pad2(date.getDate())}`
}

function parseKey(key: string): Date | null {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(key)
  if (!match)
    return null

  const date = new Date(Number(match[1]), Number(match[2]) - 1, Number(match[3]))
  return Number.isNaN(date.getTime()) ? null : date
}

function addDays(date: Date, delta: number) {
  const next = new Date(date)
  next.setDate(next.getDate() + delta)
  return next
}

function addMonths(date: Date, delta: number) {
  return new Date(date.getFullYear(), date.getMonth() + delta, 1)
}

function firstOfMonth(date: Date) {
  return new Date(date.getFullYear(), date.getMonth(), 1)
}

function monthLabelOf(date: Date) {
  return `${date.getFullYear()} 年 ${date.getMonth() + 1} 月`
}

const todayKey = dateKey(new Date())
const viewDate = shallowRef(firstOfMonth(new Date()))
const focusKey = shallowRef(todayKey)
const hoverKey = shallowRef('')

// 自定义选择的草稿：打开面板时从已提交值初始化，选完终点才向父级提交
const draftStart = shallowRef('')
const draftEnd = shallowRef('')

const hasAppliedRange = computed(() => Boolean(props.start && props.end))

const triggerLabel = computed(() => {
  if (!hasAppliedRange.value)
    return '自定义'

  const [startYear, startMonth, startDay] = props.start.split('-')
  const [endYear, endMonth, endDay] = props.end.split('-')
  return startYear === endYear
    ? `${startMonth}-${startDay} ~ ${endMonth}-${endDay}`
    : `${props.start} ~ ${props.end}`
})

const triggerAriaLabel = computed(() => hasAppliedRange.value
  ? `${props.ariaLabel} ${props.start} 至 ${props.end}`
  : props.ariaLabel)

// 选了起点未选终点时，以悬停（焦点兜底）日期预览范围；刚选中起点时焦点就在起点，不会出现误导性高亮
const previewEndKey = computed(() => {
  if (!draftStart.value || draftEnd.value)
    return ''
  return hoverKey.value || focusKey.value
})

const selectedRange = computed(() => {
  if (!draftStart.value)
    return null

  const rangeEnd = draftEnd.value || previewEndKey.value
  if (!rangeEnd)
    return { lo: draftStart.value, hi: draftStart.value }

  return draftStart.value <= rangeEnd
    ? { lo: draftStart.value, hi: rangeEnd }
    : { lo: rangeEnd, hi: draftStart.value }
})

const maxEndKey = computed(() => {
  if (!draftStart.value || draftEnd.value)
    return ''

  const start = parseKey(draftStart.value)
  return start ? dateKey(addDays(start, props.maxRangeDays - 1)) : ''
})

const viewStartKey = computed(() => dateKey(viewDate.value))
const viewEndExclusiveKey = computed(() => dateKey(addMonths(viewDate.value, visibleMonths.value)))
const nextViewDate = computed(() => addMonths(viewDate.value, 1))

const summaryText = computed(() => {
  if (!draftStart.value)
    return '选择开始日期'
  if (!draftEnd.value)
    return `${draftStart.value} ~ 选择结束日期`

  const start = parseKey(draftStart.value)
  const end = parseKey(draftEnd.value)
  const dayCount = start && end
    ? Math.round((end.getTime() - start.getTime()) / 86_400_000) + 1
    : 0
  return `${draftStart.value} ~ ${draftEnd.value} · 共 ${dayCount} 天`
})

const weekdayLabels = ['一', '二', '三', '四', '五', '六', '日']

function buildCells(monthFirst: Date, hide: 'leading' | 'trailing' | 'none' = 'none'): DayCell[] {
  // 周一开始：把当月 1 日回退到所在周周一
  const gridStart = addDays(monthFirst, -(monthFirst.getDay() + 6) % 7)
  const range = selectedRange.value

  return Array.from({ length: 42 }, (_, index) => {
    const date = addDays(gridStart, index)
    const key = dateKey(date)
    const isStart = key === draftStart.value
    const isEnd = !!draftEnd.value && key === draftEnd.value
    const inViewMonth = date.getMonth() === monthFirst.getMonth()
    const before = date < monthFirst
    return {
      key,
      day: date.getDate(),
      inViewMonth,
      isToday: key === todayKey,
      isStart,
      isEnd,
      inRange: !!range && key >= range.lo && key <= range.hi && !isStart && !isEnd,
      disabled: key > todayKey || (!!maxEndKey.value && key > maxEndKey.value),
      placeholder: !inViewMonth && (before ? hide === 'leading' : hide === 'trailing'),
    }
  })
}

// 双月并排时按相邻网格去重：左网格隐藏右月的尾随日，右网格隐藏左月的引导日
const leftCells = computed(() => buildCells(viewDate.value, visibleMonths.value > 1 ? 'trailing' : 'none'))
const rightCells = computed(() => buildCells(nextViewDate.value, 'leading'))

const triggerClasses = computed(() => [
  'relative inline-flex h-cp-control max-w-60 min-w-0 items-center gap-2 overflow-visible rounded-cp border-0 px-3.5 text-left text-cp font-emphasis leading-none whitespace-nowrap shadow-cp-input outline-none transition-[background-color,box-shadow,color] duration-[160ms] motion-reduce:transition-none',
  hasAppliedRange.value ? 'pr-9' : '',
  props.disabled
    ? 'cursor-not-allowed bg-cp-bg-container-disabled text-cp-text-disabled shadow-none'
    : open.value
      ? 'cursor-pointer bg-(--cp-input-active-bg) text-cp-text shadow-cp-input-active'
      : [
          'cursor-pointer bg-[var(--cp-input-bg)] text-cp-text',
          'hover:not-focus-within:bg-[var(--cp-input-hover-bg)] hover:not-focus-within:shadow-cp-input-hover',
          'focus-within:bg-(--cp-input-active-bg) focus-within:shadow-cp-input-active',
        ],
])

function dayClasses(cell: DayCell) {
  const base = 'relative flex size-8 items-center justify-center rounded-cp-sm border-0 p-0 text-cp outline-none transition-colors motion-reduce:transition-none focus-visible:ring-2 focus-visible:ring-cp-control-outline'
  if (cell.disabled)
    return [base, 'cursor-not-allowed bg-transparent text-cp-text-disabled']
  if (cell.isStart || cell.isEnd)
    return [base, 'cursor-pointer bg-(--cp-button-primary-bg) font-emphasis text-(--cp-button-primary-color) hover:bg-(--cp-button-primary-hover-bg)']
  if (cell.inRange)
    return [base, 'cursor-pointer bg-cp-primary-container text-cp-primary-on-container hover:bg-cp-primary-container-hover']

  return [
    base,
    'cursor-pointer bg-transparent hover:bg-cp-bg-text-hover',
    cell.inViewMonth ? 'text-cp-text' : 'text-cp-text-quaternary',
  ]
}

function focusCell(key: string) {
  panelRef.value?.querySelector<HTMLButtonElement>(`[data-day="${key}"]`)?.focus()
}

// 焦点或选中日期移出可视窗口时平移视图，双月时保持焦点月尽量靠右
function syncViewToKey(key: string) {
  if (key < viewStartKey.value) {
    viewDate.value = firstOfMonth(parseKey(key) ?? new Date())
    return
  }
  if (key >= viewEndExclusiveKey.value) {
    const month = firstOfMonth(parseKey(key) ?? new Date())
    viewDate.value = visibleMonths.value > 1 ? addMonths(month, -1) : month
  }
}

function shiftMonth(delta: number) {
  // 焦点日期跟随月份切换并钳制到月末，且不落入禁用日，保证网格始终可 Tab 进入
  const current = parseKey(focusKey.value) ?? new Date()
  const target = new Date(current.getFullYear(), current.getMonth() + delta, 1)
  const daysInTarget = new Date(target.getFullYear(), target.getMonth() + 1, 0).getDate()
  target.setDate(Math.min(current.getDate(), daysInTarget))
  let targetKey = dateKey(target)
  if (targetKey > todayKey)
    targetKey = todayKey
  if (maxEndKey.value && targetKey > maxEndKey.value)
    targetKey = maxEndKey.value
  focusKey.value = targetKey
  hoverKey.value = ''
  viewDate.value = addMonths(viewDate.value, delta)
}

function choose(cell: DayCell) {
  if (props.disabled || cell.disabled)
    return

  hoverKey.value = ''
  focusKey.value = cell.key

  if (!draftStart.value || draftEnd.value) {
    draftStart.value = cell.key
    draftEnd.value = ''
  }
  else if (cell.key >= draftStart.value) {
    emit('custom', draftStart.value, cell.key)
    open.value = false
  }
  else {
    draftStart.value = cell.key
    draftEnd.value = ''
  }

  syncViewToKey(cell.key)
}

function clearRange() {
  draftStart.value = ''
  draftEnd.value = ''
  emit('clear')
  open.value = false
}

function handleCellMouseenter(cell: DayCell) {
  if (!cell.disabled)
    hoverKey.value = cell.key
}

function handleCellKeydown(event: KeyboardEvent) {
  const delta = { ArrowLeft: -1, ArrowRight: 1, ArrowUp: -7, ArrowDown: 7 }[event.key]
  if (delta === undefined)
    return

  event.preventDefault()
  const next = addDays(parseKey(focusKey.value) ?? new Date(), delta)
  const nextKey = dateKey(next)
  // 禁用区（未来日期、超出范围上限）不进入，避免键盘焦点落入不可选日期
  if (nextKey > todayKey || (maxEndKey.value && nextKey > maxEndKey.value))
    return

  focusKey.value = nextKey
  syncViewToKey(nextKey)
  void nextTick(() => focusCell(focusKey.value))
}

// 悬停监听放在脚本侧（同 BasePopover），避免在静态容器上声明模板事件
useEventListener(panelRef, 'mouseleave', () => {
  hoverKey.value = ''
})

watch(open, async (isOpen) => {
  if (isOpen) {
    draftStart.value = props.start
    draftEnd.value = props.end
    const start = parseKey(props.start)
    const base = parseKey(props.end) ?? start ?? new Date()
    let month = firstOfMonth(base)
    // 双月时让基准月落在右侧，覆盖上月上下文；范围跨月时从起始月开始，保证已选范围可见
    if (visibleMonths.value > 1) {
      month = addMonths(month, -1)
      if (start && firstOfMonth(start) < month)
        month = firstOfMonth(start)
    }
    viewDate.value = month
    focusKey.value = dateKey(base)
    hoverKey.value = ''
    await nextTick()
    focusCell(focusKey.value)
    return
  }

  // 焦点仍在面板内时还给触发按钮，避免 Esc 或选中后焦点丢失
  if (panelRef.value?.contains(document.activeElement))
    triggerRef.value?.focus()
})
</script>

<template>
  <BasePopover v-model="open" v-bind="$attrs" placement="bottom-end" :disabled="disabled">
    <template #trigger>
      <button
        ref="triggerRef"
        type="button"
        :class="triggerClasses"
        :disabled="disabled"
        :aria-label="triggerAriaLabel"
        aria-haspopup="dialog"
        :aria-expanded="open"
      >
        <Calendar
          class="shrink-0"
          :class="disabled ? 'text-cp-text-disabled' : hasAppliedRange || open ? 'text-cp-primary-text' : 'text-cp-text-quaternary'"
          :size="16"
        />
        <span class="min-w-0 flex-1 truncate" :class="hasAppliedRange ? 'font-mono tabular-nums' : 'text-cp-text-secondary'">
          {{ triggerLabel }}
        </span>
        <ChevronDown
          v-if="!hasAppliedRange"
          class="shrink-0 text-cp-text-quaternary transition-transform duration-150 motion-reduce:transition-none"
          :class="open ? 'rotate-180' : ''"
          :size="14"
        />
      </button>
      <button
        v-if="hasAppliedRange"
        type="button"
        aria-label="清除自定义范围"
        class="absolute top-1/2 right-2 flex size-5 -translate-y-1/2 cursor-pointer items-center justify-center rounded-cp-sm border-0 bg-transparent p-0 text-cp-text-quaternary outline-none transition-colors motion-reduce:transition-none hover:bg-cp-bg-text-hover hover:text-cp-text focus-visible:ring-2 focus-visible:ring-cp-control-outline"
        :disabled="disabled"
        @click.stop="clearRange"
      >
        <X :size="13" />
      </button>
    </template>

    <div
      ref="panelRef"
      role="dialog"
      :aria-label="ariaLabel"
      class="w-max max-w-[calc(100vw-24px)]"
    >
      <div class="flex">
        <div class="p-2">
          <div class="flex items-center justify-between px-1 pb-1">
            <BaseIconButton variant="ghost" size="sm" label="上个月" @click="shiftMonth(-1)">
              <ChevronLeft class="size-3.5" />
            </BaseIconButton>
            <span class="text-cp font-emphasis text-cp-text">{{ monthLabelOf(viewDate) }}</span>
            <BaseIconButton v-if="!wideViewport" variant="ghost" size="sm" label="下个月" @click="shiftMonth(1)">
              <ChevronRight class="size-3.5" />
            </BaseIconButton>
            <span v-else class="size-7" aria-hidden="true" />
          </div>

          <div class="grid grid-cols-7 gap-0.5" aria-hidden="true">
            <span
              v-for="weekday in weekdayLabels"
              :key="weekday"
              class="flex size-8 items-center justify-center text-cp-xs text-cp-text-tertiary"
            >
              {{ weekday }}
            </span>
          </div>
          <div
            class="grid grid-cols-7 gap-0.5"
            role="group"
            :aria-label="monthLabelOf(viewDate)"
          >
            <template v-for="cell in leftCells" :key="cell.key">
              <span v-if="cell.placeholder" class="size-8" aria-hidden="true" />
              <button
                v-else
                type="button"
                :data-day="cell.key"
                :class="dayClasses(cell)"
                :tabindex="cell.key === focusKey && !cell.disabled ? 0 : -1"
                :disabled="cell.disabled"
                :aria-label="cell.key"
                :aria-current="cell.isToday ? 'date' : undefined"
                @click="choose(cell)"
                @mouseenter="handleCellMouseenter(cell)"
                @focus="focusKey = cell.key"
                @keydown="handleCellKeydown"
              >
                <span>{{ cell.day }}</span>
                <span
                  v-if="cell.isToday"
                  class="absolute bottom-0.75 left-1/2 size-1 -translate-x-1/2 rounded-full bg-current"
                />
              </button>
            </template>
          </div>
        </div>

        <div v-if="wideViewport" class="border-l border-cp-split p-2">
          <div class="flex items-center justify-between px-1 pb-1">
            <span class="size-7" aria-hidden="true" />
            <span class="text-cp font-emphasis text-cp-text">{{ monthLabelOf(nextViewDate) }}</span>
            <BaseIconButton variant="ghost" size="sm" label="下个月" @click="shiftMonth(1)">
              <ChevronRight class="size-3.5" />
            </BaseIconButton>
          </div>

          <div class="grid grid-cols-7 gap-0.5" aria-hidden="true">
            <span
              v-for="weekday in weekdayLabels"
              :key="weekday"
              class="flex size-8 items-center justify-center text-cp-xs text-cp-text-tertiary"
            >
              {{ weekday }}
            </span>
          </div>
          <div
            class="grid grid-cols-7 gap-0.5"
            role="group"
            :aria-label="monthLabelOf(nextViewDate)"
          >
            <template v-for="cell in rightCells" :key="cell.key">
              <span v-if="cell.placeholder" class="size-8" aria-hidden="true" />
              <button
                v-else
                type="button"
                :data-day="cell.key"
                :class="dayClasses(cell)"
                :tabindex="cell.key === focusKey && !cell.disabled ? 0 : -1"
                :disabled="cell.disabled"
                :aria-label="cell.key"
                :aria-current="cell.isToday ? 'date' : undefined"
                @click="choose(cell)"
                @mouseenter="handleCellMouseenter(cell)"
                @focus="focusKey = cell.key"
                @keydown="handleCellKeydown"
              >
                <span>{{ cell.day }}</span>
                <span
                  v-if="cell.isToday"
                  class="absolute bottom-0.75 left-1/2 size-1 -translate-x-1/2 rounded-full bg-current"
                />
              </button>
            </template>
          </div>
        </div>
      </div>

      <div class="flex flex-wrap items-center justify-between gap-x-3 gap-y-1 border-t border-cp-split px-3 py-2">
        <p class="m-0 min-w-0 text-cp-xs leading-4.5 text-cp-text-secondary" aria-live="polite">
          {{ summaryText }}
        </p>
        <div class="flex shrink-0 items-center gap-3">
          <span
            class="text-cp-xs text-cp-text-quaternary"
            title="按部署时区统计，结束日期包含当天"
          >
            按部署时区 · 含结束当天
          </span>
          <button
            v-if="hasAppliedRange"
            type="button"
            class="cursor-pointer rounded-cp-sm border-0 bg-transparent p-0 text-cp-xs font-emphasis text-cp-primary-text outline-none transition-colors hover:text-cp-primary-text-hover focus-visible:ring-2 focus-visible:ring-cp-control-outline"
            @click="clearRange"
          >
            清除范围
          </button>
        </div>
      </div>
    </div>
  </BasePopover>
</template>
