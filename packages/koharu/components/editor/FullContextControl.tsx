'use client'

import { Globe } from 'lucide-react'
import { useTranslation } from 'react-i18next'

import { call } from '@/lib/backend'
import { usePage } from '@/lib/queries'
import { useKoharuStore } from '@/lib/store'
import { commands } from '@koharu/bridge/protocol'
import { Button } from '@koharu/ui/components/button'

export function FullContextControl({ disabled }: { disabled: boolean }) {
  const { t } = useTranslation()
  const page = usePage().data
  const jobs = useKoharuStore((state) => state.jobs)
  const running = Object.values(jobs).find((job) => job.state === 'running')

  return (
    <Button
      type='button'
      size='sm'
      variant='outline'
      className='rounded-lg px-2.5 text-[11px]'
      disabled={disabled || Boolean(running) || !page}
      aria-label={t('inference.fullContext')}
      onClick={() => void call(commands.fullContextTranslate).catch(() => undefined)}
    >
      <Globe className='size-3' />
      <span>{t('inference.fullContext')}</span>
    </Button>
  )
}
