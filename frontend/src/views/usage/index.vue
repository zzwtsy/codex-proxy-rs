<script setup lang="ts">
import type { TimeRangePreset } from '@/composables/useTimeRange'

import { BaseCard, BaseIconButton, BasePageHeader, BaseSegmented, BaseTableColumnSettings, BaseTablePagination, useTableColumns } from '@codex-proxy/ui'
import { Eye } from '@lucide/vue'
import { computed, shallowRef, watch } from 'vue'
import DateRangePicker from '@/components/DateRangePicker.vue'
import ProviderFilter from '@/components/ProviderFilter.vue'
import { maxCustomRangeDays, timeRangePresets, useTimeRange } from '@/composables/useTimeRange'
import { usageRecordColumns } from '@/components/usage/shared/columns'
import UsageRecordsTable from '@/components/usage/UsageRecordsTable.vue'
import OpsErrorPanel from './components/OpsErrorPanel.vue'
import UsageFilters from './components/UsageFilters.vue'
import UsageInsightsGrid from './components/UsageInsightsGrid.vue'
import UsageRecordDetailModal from './components/UsageRecordDetailModal.vue'
import UsageSummaryCards from './components/UsageSummaryCards.vue'
import { useUsageRecordDetail } from './composables/useUsageRecordDetail'
import { useUsageRecordsTable } from './composables/useUsageRecordsTable'

const recordView = shallowRef('success')
const { visibleColumns, columnOptions, setColumnVisible, setColumnOrder, resetColumns } = useTableColumns(usageRecordColumns, 'usage-records')
const recordViewOptions = [
  { label: '成功记录', value: 'success' },
  { label: '错误排查', value: 'errors' },
]
const {
  timeRange,
  timeRangeParams,
  customStartDate,
  customEndDate,
  selectPreset,
  selectCustomRange,
  clearCustomRange,
  latestTimeRangeParams,
} = useTimeRange()

// 分段控件只承载预设；自定义生效时进入无选中态，由日期 chip 呈现当前范围
const presetSelection = computed<string>({
  get: () => (timeRange.value === 'custom' ? '' : timeRange.value),
  set: value => selectPreset(value as TimeRangePreset),
})

const {
  currentPage,
  searchQuery,
  providerQuery,
  usagePagination,
  loading,
  error,
  analyticsLoading,
  records,
  summary,
  insights,
  refreshingList,
  diagnosticDimension,
  loadUsageRecords,
  refreshUsageRecords,
  handlePageChange,
  handlePageSizeChange,
} = useUsageRecordsTable({
  timeRangeParams,
  latestTimeRangeParams,
  active: computed(() => recordView.value === 'success'),
})

const { showDetailModal, selectedUsageRecord, handleViewDetail } = useUsageRecordDetail()

watch(timeRangeParams, () => {
  currentPage.value = 1
  void loadUsageRecords()
})
</script>

<template>
  <div class="w-full">
    <BasePageHeader title="使用统计" description="查看请求用量、性能趋势与调用错误记录">
      <template #actions>
        <div class="flex min-w-0 max-w-[calc(100vw-32px)] flex-wrap items-center justify-end gap-2 max-[960px]:justify-start">
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
          <ProviderFilter
            v-model="providerQuery"
            :disabled="refreshingList"
            class="shrink-0"
          />
        </div>
      </template>
    </BasePageHeader>

    <UsageSummaryCards :summary="summary" />
    <UsageInsightsGrid
      v-model:diagnostic-dimension="diagnosticDimension"
      :overview="insights.overview"
      :diagnostics="insights.diagnostics"
      :loading="analyticsLoading"
    />

    <BaseCard
      class="mt-5 flex flex-col"
    >
      <template #header>
        <div class="flex flex-wrap items-center justify-between gap-3">
          <div>
            <h2 class="m-0 text-xl leading-[1.15] font-heavy text-cp-text">
              请求明细
            </h2>
            <p
              class="mt-1.75 mb-0 text-cp leading-[1.15] font-emphasis text-cp-text-secondary"
            >
              成功请求与失败请求明细
            </p>
          </div>
          <BaseSegmented v-model="recordView" label="请求明细类型" :options="recordViewOptions" class="w-52" />
        </div>
      </template>

      <template #body>
        <div
          v-show="recordView === 'success'"
          class="grid min-h-130 min-w-0 flex-1 grid-rows-[auto_minmax(0,1fr)] gap-3"
        >
          <UsageFilters
            v-model:search="searchQuery"
            :loading="loading"
            :refreshing="refreshingList"
            @refresh="refreshUsageRecords"
          >
            <template #actions>
              <BaseTableColumnSettings
                :options="columnOptions"
                @change="setColumnVisible"
                @reorder="setColumnOrder"
                @reset="resetColumns"
              />
            </template>
          </UsageFilters>

          <div class="flex min-h-0 min-w-0 flex-col">
            <UsageRecordsTable
              class="min-h-0 flex-1"
              :columns="visibleColumns"
              :rows="records"
              :loading="loading"
              :empty-text="error ? `加载失败：${error}` : '暂无使用记录'"
            >
              <template #actions="{ row }">
                <div class="flex items-center justify-start">
                  <BaseIconButton
                    variant="ghost"
                    size="sm"
                    label="查看使用记录详情"
                    @click="handleViewDetail(row)"
                  >
                    <Eye class="size-3.5" />
                  </BaseIconButton>
                </div>
              </template>
            </UsageRecordsTable>
            <BaseTablePagination
              :pagination="usagePagination"
              :loading="loading"
              @page-change="handlePageChange"
              @page-size-change="handlePageSizeChange"
            />
          </div>
        </div>

        <div v-show="recordView === 'errors'" class="min-h-130 min-w-0 flex-1">
          <OpsErrorPanel
            :time-range-params="timeRangeParams"
            :latest-time-range-params="latestTimeRangeParams"
            :provider="providerQuery"
            :active="recordView === 'errors'"
          />
        </div>
      </template>
    </BaseCard>

    <UsageRecordDetailModal v-model="showDetailModal" :record="selectedUsageRecord" />
  </div>
</template>
