<script setup lang="ts">
import type { AccountSharePerspective } from '../composables/useAccountShare'
import type { TimeRangePreset } from '@/composables/useTimeRange'

import { BaseCard, BaseEmpty, BaseSegmented, BaseSelect, BaseSkeleton, BaseTable, defineTableColumns } from '@codex-proxy/ui'
import { computed } from 'vue'
import DateRangePicker from '@/components/DateRangePicker.vue'
import { maxCustomRangeDays, timeRangePresets } from '@/composables/useTimeRange'
import { formatLocalizedCompactNumber as formatCompactNumber, formatInteger, formatPercent } from '@/utils/format'

import { useAccountShare } from '../composables/useAccountShare'

const {
  loading,
  perspective,
  selectedId,
  entityOptions,
  subject,
  rows,
  setPerspective,
  selectEntity,
  timeRange,
  customStartDate,
  customEndDate,
  selectPreset,
  selectCustomRange,
  clearCustomRange,
} = useAccountShare()

const perspectiveOptions = [
  { label: '按账号', value: 'account' },
  { label: '按密钥', value: 'key' },
]

const perspectiveModel = computed<string>({
  get: () => perspective.value,
  set: value => setPerspective(value as AccountSharePerspective),
})

// 选中实体为空时让下拉显示「自动」实体的 id，与展示保持一致
const selectedEntityId = computed(() => selectedId.value || entityOptions.value[0]?.value || '')

const selectedIdModel = computed<string>({
  get: () => selectedEntityId.value,
  set: value => selectEntity(value),
})

// 分段控件只承载预设；自定义生效时进入无选中态，由日期 chip 呈现当前范围
const presetSelection = computed<string>({
  get: () => (timeRange.value === 'custom' ? '' : timeRange.value),
  set: value => selectPreset(value as TimeRangePreset),
})

const byAccount = computed(() => perspective.value === 'account')
const counterpartNameLabel = computed(() => (byAccount.value ? 'Client Key' : '账号'))
const counterpartCountCaption = computed(() => {
  const count = subject.value?.counterpartCount ?? 0
  return byAccount.value ? `共 ${count} 个 Client Key` : `共 ${count} 个账号`
})
const controlsDisabled = computed(() => loading.value || entityOptions.value.length === 0)

const shareColumns = computed(() => defineTableColumns<AccountShareRowType>([
  { key: 'name', label: counterpartNameLabel.value, kind: 'custom', size: '2xl' },
  { key: 'totalTokens', label: 'Token 用量', kind: 'numeric', size: 'md' },
  { key: 'share', label: '占比', kind: 'custom', size: '3xl', align: 'right' },
]))

type AccountShareRowType = (typeof rows.value)[number]

function shareBarWidth(share: number) {
  return `${Math.round(Math.min(1, Math.max(0, share)) * 100)}%`
}

function shareLabel(row: AccountShareRowType) {
  const percent = formatPercent(row.share)
  return byAccount.value
    ? `Token 用量 ${formatInteger(row.totalTokens)}，占该账号 ${percent}`
    : `Token 用量 ${formatInteger(row.totalTokens)}，占该密钥 ${percent}`
}
</script>

<template>
  <BaseCard
    as="article"
    title="账号分摊"
    description="选择账号或密钥，查看 Token 用量构成与占比"
    class="mt-4 min-w-0 w-full"
  >
    <template #body>
      <div class="grid min-w-0 gap-4">
        <div class="flex min-w-0 flex-wrap items-center gap-2">
          <BaseSegmented
            v-model="perspectiveModel"
            label="分摊视角"
            :options="perspectiveOptions"
            :disabled="loading"
            class="w-40 shrink-0"
          />
          <BaseSelect
            v-model="selectedIdModel"
            :options="entityOptions"
            :disabled="controlsDisabled"
            class="w-72 min-w-0"
            aria-label="选择分摊实体"
          />
          <div class="ms-auto flex min-w-0 flex-wrap items-center justify-end gap-2">
            <BaseSegmented
              v-model="presetSelection"
              label="快捷时间范围"
              :options="timeRangePresets"
              class="w-72 shrink-0"
            />
            <DateRangePicker
              :start="customStartDate"
              :end="customEndDate"
              :max-range-days="maxCustomRangeDays"
              class="shrink-0"
              aria-label="自定义时间范围"
              @custom="selectCustomRange"
              @clear="clearCustomRange"
            />
          </div>
        </div>

        <div v-if="loading" class="grid gap-3 py-1" aria-hidden="true">
          <BaseSkeleton shape="text" class="h-4 w-56" />
          <BaseSkeleton v-for="width in ['w-3/4', 'w-2/3', 'w-1/2']" :key="width" shape="text" class="h-3.5" :class="width" />
        </div>

        <BaseEmpty
          v-else-if="!subject"
          size="sm"
          surface="none"
          title="暂无分摊数据"
          description="当前范围没有 Codex 账号的密钥用量"
          class="min-h-40 place-content-center"
        />

        <template v-else>
          <div class="flex min-w-0 flex-wrap items-end justify-between gap-x-4 gap-y-2">
            <div class="min-w-0">
              <code
                class="block max-w-full truncate font-mono text-cp-lg leading-tight font-heavy text-cp-text"
                :title="subject.name"
              >
                {{ subject.name }}
              </code>
              <p class="m-0 mt-1 text-cp-sm leading-none font-emphasis text-cp-text-quaternary">
                {{ counterpartCountCaption }}
              </p>
            </div>
            <div class="shrink-0 text-right">
              <strong
                class="block font-mono text-[26px] leading-none font-extrabold text-cp-text"
                :title="`Token 总量 ${formatInteger(subject.totalTokens)}`"
              >
                {{ formatCompactNumber(subject.totalTokens) }}
              </strong>
              <p class="m-0 mt-1.5 text-cp-sm leading-none font-emphasis text-cp-text-quaternary">
                Token 总量
              </p>
            </div>
          </div>

          <BaseTable
            class="max-h-105 min-w-0 w-full"
            :columns="shareColumns"
            :rows="rows"
            density="compact"
            row-key="key"
            empty-text="暂无构成分布"
          >
            <template #name="{ row }">
              <code
                class="block max-w-full truncate font-mono text-cp-sm leading-none font-bold text-cp-text"
                :title="row.name"
              >
                {{ row.name }}
              </code>
            </template>

            <template #totalTokens="{ row }">
              <strong
                class="font-mono font-bold tabular-nums text-cp-text"
                :title="formatInteger(row.totalTokens)"
              >
                {{ formatCompactNumber(row.totalTokens) }}
              </strong>
            </template>

            <template #share="{ row }">
              <span
                class="flex items-center justify-end gap-2 font-mono leading-none tabular-nums"
                :aria-label="shareLabel(row)"
                :title="shareLabel(row)"
              >
                <span aria-hidden="true" class="h-1.5 w-full max-w-48 flex-1 overflow-hidden rounded-full bg-cp-fill-secondary">
                  <span class="block h-full rounded-full bg-cp-info" :style="{ width: shareBarWidth(row.share) }" />
                </span>
                <strong class="w-11 shrink-0 whitespace-nowrap text-right text-cp-sm font-bold text-cp-text">
                  {{ formatPercent(row.share) }}
                </strong>
              </span>
            </template>
          </BaseTable>
        </template>
      </div>
    </template>
  </BaseCard>
</template>
