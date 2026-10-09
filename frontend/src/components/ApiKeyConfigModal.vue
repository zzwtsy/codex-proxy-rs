<script setup lang="ts">
import { BaseButton, BaseIconButton, BaseModal, BaseScrollbar, BaseSegmented, BaseSwitch } from '@codex-proxy/ui'

import { Apple, Copy, Monitor, Upload } from '@lucide/vue'
import { computed, shallowRef } from 'vue'
import { buildCodexCcSwitchImportDeeplink, buildCodexConfig, CODEX_WEBSOCKET_ENABLED_BY_DEFAULT } from '@/utils/client'

const props = withDefaults(defineProps<{
  title?: string
  apiKey: {
    name?: string
    key?: string
  } | null
  apiBaseUrl: string
}>(), { title: '使用密钥' })

const emit = defineEmits<{
  copy: [text: string]
}>()

const open = defineModel<boolean>({ default: false })

const activePlatform = shallowRef('unix')
const websocketEnabled = shallowRef(CODEX_WEBSOCKET_ENABLED_BY_DEFAULT)

const platformOptions = [
  { label: 'macOS / Linux', value: 'unix', icon: Apple },
  { label: 'Windows', value: 'windows', icon: Monitor },
]

const keyValue = computed(() => props.apiKey?.key ?? '')
const configPath = computed(() =>
  activePlatform.value === 'windows'
    ? '%userprofile%\\.codex\\config.toml'
    : '~/.codex/config.toml',
)
const codexConfig = computed(() => buildCodexConfig({
  apiKey: keyValue.value,
  baseUrl: props.apiBaseUrl,
  websocketEnabled: websocketEnabled.value,
}))

function importToCcs() {
  if (!keyValue.value)
    return
  window.location.href = buildCodexCcSwitchImportDeeplink({
    apiKey: keyValue.value,
    baseUrl: props.apiBaseUrl,
    providerName: props.apiKey?.name || 'codex-proxy-rs',
  })
}
</script>

<template>
  <BaseModal
    v-model="open"
    :title="title"
    description="保存或合并下方配置后重启 Codex"
    size="lg"
  >
    <div class="flex flex-col gap-5">
      <div class="flex flex-wrap items-center justify-between gap-3">
        <BaseSegmented v-model="activePlatform" label="配置平台" :options="platformOptions" />
        <BaseSwitch
          v-model="websocketEnabled"
          label="切换 WebSocket 配置"
          active-text="WS"
          inactive-text="WS"
          inline-prompt
          :width="56"
        />
      </div>

      <section class="overflow-hidden rounded-cp-card bg-cp-fill-quaternary shadow-cp-tertiary">
        <div class="flex items-center justify-between gap-3 px-4 py-2.5">
          <span
            class="min-w-0 truncate font-mono text-cp-sm font-emphasis text-cp-text-secondary"
          >
            {{ configPath }}
          </span>
          <BaseIconButton
            variant="secondary"
            size="sm"
            label="复制"
            @click="emit('copy', codexConfig)"
          >
            <Copy class="size-3.5" />
          </BaseIconButton>
        </div>
        <BaseScrollbar max-height="calc(100dvh - 21rem)">
          <div class="mx-3 mb-3 rounded-cp bg-cp-bg-container px-3.5 py-3 shadow-cp-tertiary">
            <pre
              class="m-0 whitespace-pre-wrap wrap-break-word font-mono text-cp-sm leading-[1.65] font-emphasis text-cp-text"
              v-text="codexConfig"
            />
          </div>
        </BaseScrollbar>
      </section>
    </div>

    <template #footer>
      <BaseButton variant="secondary" :disabled="!keyValue" @click="importToCcs">
        <Upload class="size-4" />
        导入 CCSwitch
      </BaseButton>
      <BaseButton variant="primary" @click="open = false">
        关闭
      </BaseButton>
    </template>
  </BaseModal>
</template>
