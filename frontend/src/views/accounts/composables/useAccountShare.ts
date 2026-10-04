import { computed, onMounted, onScopeDispose, shallowRef, watch } from 'vue'
import { getUsageRecordInsightsDiagnostics } from '@/api'
import { useTimeRange } from '@/composables/useTimeRange'

type AccountShareItem = Awaited<ReturnType<typeof getUsageRecordInsightsDiagnostics>>['items'][number]

export type AccountSharePerspective = 'account' | 'key'

export interface AccountShareEntityOption {
  label: string
  value: string
}

export interface AccountShareSubject {
  name: string
  totalTokens: number
  counterpartCount: number
}

export interface AccountShareRow {
  key: string
  name: string
  totalTokens: number
  share: number
}

interface ShareEntity {
  id: string
  name: string
  totalTokens: number
  counterparts: Map<string, { id: string, name: string, totalTokens: number }>
}

// 「账号分摊」数据：聚焦单个账号或密钥，呈现其 Token 用量构成
// 后端在 accountKey 维度返回全部组合（不截断）；占比分母跟随选中实体，由前端合计
export function useAccountShare() {
  const timeRangeApi = useTimeRange()
  const loading = shallowRef(true)
  const items = shallowRef<AccountShareItem[]>([])
  const perspective = shallowRef<AccountSharePerspective>('account')
  // 空值表示「自动选中该视角总量最大者」，实体在时间范围变化后消失时同样回退
  const selectedId = shallowRef('')
  let requestId = 0
  let controller: AbortController | undefined
  let disposed = false

  const entities = computed(() => buildEntities(items.value, perspective.value))
  const selectedEntity = computed(
    () => entities.value.find(entity => entity.id === selectedId.value) ?? entities.value[0] ?? null,
  )
  const entityOptions = computed<AccountShareEntityOption[]>(() =>
    entities.value.map(entity => ({ label: entity.name, value: entity.id })),
  )
  const subject = computed<AccountShareSubject | null>(() => {
    const entity = selectedEntity.value
    return entity
      ? { name: entity.name, totalTokens: entity.totalTokens, counterpartCount: entity.counterparts.size }
      : null
  })
  const rows = computed<AccountShareRow[]>(() => {
    const entity = selectedEntity.value
    if (!entity)
      return []

    return [...entity.counterparts.values()]
      .map(part => ({
        key: `${entity.id}::${part.id}`,
        name: part.name,
        totalTokens: part.totalTokens,
        share: entity.totalTokens > 0 ? part.totalTokens / entity.totalTokens : 0,
      }))
      .sort((left, right) => right.totalTokens - left.totalTokens || left.name.localeCompare(right.name))
  })

  async function loadAccountShare() {
    const id = ++requestId
    controller?.abort()
    controller = new AbortController()
    loading.value = true
    try {
      const result = await getUsageRecordInsightsDiagnostics({
        ...timeRangeApi.timeRangeParams.value,
        dimension: 'accountKey',
      }, { signal: controller.signal })
      if (id !== requestId || disposed)
        return
      items.value = result.items
    }
    catch {}
    finally {
      if (id === requestId)
        loading.value = false
    }
  }

  function setPerspective(next: AccountSharePerspective) {
    if (next === perspective.value)
      return
    perspective.value = next
    selectedId.value = ''
  }

  function selectEntity(id: string) {
    selectedId.value = id
  }

  onMounted(() => loadAccountShare())

  watch(timeRangeApi.timeRangeParams, () => {
    void loadAccountShare()
  })

  onScopeDispose(() => {
    disposed = true
    requestId += 1
    controller?.abort()
  })

  return {
    loading,
    perspective,
    selectedId,
    entityOptions,
    subject,
    rows,
    setPerspective,
    selectEntity,
    ...timeRangeApi,
  }
}

export type UseAccountShare = ReturnType<typeof useAccountShare>

function buildEntities(items: AccountShareItem[], perspective: AccountSharePerspective): ShareEntity[] {
  const byEntity = new Map<string, ShareEntity>()
  for (const item of items) {
    const byAccount = perspective === 'account'
    const entityId = (byAccount ? item.accountId : item.clientApiKeyId) ?? ''
    const entityName = byAccount
      ? item.accountName || item.accountId || '未知账号'
      : item.clientApiKeyName || item.clientApiKeyId || '未知密钥'
    const partId = (byAccount ? item.clientApiKeyId : item.accountId) ?? ''
    const partName = byAccount
      ? item.clientApiKeyName || item.clientApiKeyId || '未知密钥'
      : item.accountName || item.accountId || '未知账号'

    let entity = byEntity.get(entityId)
    if (!entity) {
      entity = { id: entityId, name: entityName, totalTokens: 0, counterparts: new Map() }
      byEntity.set(entityId, entity)
    }
    entity.totalTokens += item.totalTokens
    const part = entity.counterparts.get(partId) ?? { id: partId, name: partName, totalTokens: 0 }
    part.totalTokens += item.totalTokens
    entity.counterparts.set(partId, part)
  }

  return [...byEntity.values()]
    .sort((left, right) => right.totalTokens - left.totalTokens || left.name.localeCompare(right.name))
}
