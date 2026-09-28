import React from 'react';
import {
  Activity,
  Radio,
  Layers,
  Database,
  Network,
  Share2,
  HardDrive,
  GitBranch,
  Send,
  Sliders,
} from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { cn } from '../ui/button';

export type PageId =
  | 'overview'
  | 'clients'
  | 'subscriptions'
  | 'retained'
  | 'listeners'
  | 'cluster'
  | 'pipeline'
  | 'tools';

interface SidebarProps {
  activePage: PageId;
  onPageChange: (page: PageId) => void;
}

export const Sidebar: React.FC<SidebarProps> = ({ activePage, onPageChange }) => {
  const { t } = useI18n();

  const menuItems: { id: PageId; labelKey: string; icon: React.FC<{ className?: string }> }[] = [
    { id: 'overview', labelKey: 'menu.overview', icon: Activity },
    { id: 'clients', labelKey: 'menu.clients', icon: Radio },
    { id: 'subscriptions', labelKey: 'menu.subscriptions', icon: Layers },
    { id: 'retained', labelKey: 'menu.retained', icon: Database },
    { id: 'listeners', labelKey: 'menu.listeners', icon: Network },
    { id: 'cluster', labelKey: 'menu.cluster', icon: Share2 },
    { id: 'pipeline', labelKey: 'menu.pipeline', icon: GitBranch },
    { id: 'tools', labelKey: 'menu.tools', icon: Send },
  ];

  return (
    <aside className="w-64 shrink-0 bg-dark-900 border-r border-dark-750/80 flex flex-col justify-between select-none">
      <div>
        {/* Brand header */}
        <div className="h-16 flex items-center px-5 gap-3 border-b border-dark-800">
          <div className="w-9 h-9 rounded-xl bg-brand-500/15 border border-brand-500/30 flex items-center justify-center text-brand-400 shadow-sm shadow-brand-500/20">
            <Sliders className="h-5 w-5" />
          </div>
          <div>
            <h1 className="text-sm font-bold tracking-tight text-white flex items-center gap-1.5">
              AirMQ
              <span className="text-[10px] uppercase font-mono px-1.5 py-0.5 rounded bg-brand-500/20 text-brand-400 border border-brand-500/30">
                v1.0
              </span>
            </h1>
            <p className="text-[11px] text-slate-400 leading-tight">工业级物联枢纽</p>
          </div>
        </div>

        {/* Navigation items */}
        <nav className="p-3 space-y-1">
          {menuItems.map((item) => {
            const Icon = item.icon;
            const isActive = activePage === item.id;
            return (
              <button
                key={item.id}
                onClick={() => onPageChange(item.id)}
                className={cn(
                  'w-full flex items-center gap-3 px-3.5 py-2.5 rounded-lg text-sm font-medium transition-all text-left group',
                  isActive
                    ? 'bg-brand-500/15 text-brand-400 border border-brand-500/30 shadow-sm shadow-brand-500/10'
                    : 'text-slate-300 hover:text-white hover:bg-dark-800/80 border border-transparent'
                )}
              >
                <Icon
                  className={cn(
                    'h-4 w-4 transition-colors',
                    isActive ? 'text-brand-400' : 'text-slate-400 group-hover:text-slate-200'
                  )}
                />
                <span className="flex-1 truncate">{t(item.labelKey)}</span>
                {isActive && (
                  <span className="w-1.5 h-1.5 rounded-full bg-brand-400 shadow-sm shadow-brand-400" />
                )}
              </button>
            );
          })}
        </nav>
      </div>

      {/* Footer Info */}
      <div className="p-4 m-3 rounded-xl border border-dark-750 bg-dark-850/60 text-xs text-slate-400 space-y-1">
        <div className="flex items-center justify-between">
          <span className="text-slate-500">Core Engine:</span>
          <span className="font-mono text-emerald-400">gnet/v2</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-slate-500">Fast-Path:</span>
          <span className="font-mono text-cyan-400">~8.6ns</span>
        </div>
        <div className="flex items-center justify-between">
          <span className="text-slate-500">QUIC RFC 9000:</span>
          <span className="font-mono text-brand-400">Active</span>
        </div>
      </div>
    </aside>
  );
};
