import React from 'react';
import { Globe, LogOut, RefreshCw, User, Server } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { Button } from '../ui/button';

interface HeaderProps {
  nodeName: string;
  username: string;
  onLogout: () => void;
  onRefresh: () => void;
  isRefreshing?: boolean;
}

export const Header: React.FC<HeaderProps> = ({
  nodeName,
  username,
  onLogout,
  onRefresh,
  isRefreshing = false,
}) => {
  const { lang, setLang, t } = useI18n();

  return (
    <header className="h-16 px-6 bg-dark-900/90 backdrop-blur-md border-b border-dark-750/80 flex items-center justify-between z-10 shrink-0">
      {/* Left: Node Info Banner */}
      <div className="flex items-center gap-3">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-dark-800 border border-dark-700 text-xs">
          <Server className="h-3.5 w-3.5 text-brand-400" />
          <span className="text-slate-400">Node:</span>
          <span className="font-mono text-white font-medium">{nodeName || 'standalone-node'}</span>
          <span className="w-2 h-2 rounded-full bg-emerald-400 animate-pulse ml-1" />
        </div>
      </div>

      {/* Right: Actions */}
      <div className="flex items-center gap-3">
        {/* Manual Refresh */}
        <Button
          variant="ghost"
          size="sm"
          onClick={onRefresh}
          className="text-xs text-slate-300 gap-1.5"
          disabled={isRefreshing}
        >
          <RefreshCw className={`h-3.5 w-3.5 ${isRefreshing ? 'animate-spin text-brand-400' : ''}`} />
          <span>{t('common.refresh')}</span>
        </Button>

        {/* Language Switcher */}
        <Button
          variant="outline"
          size="sm"
          onClick={() => setLang(lang === 'zh-CN' ? 'en-US' : 'zh-CN')}
          className="text-xs gap-1.5 border-dark-700"
        >
          <Globe className="h-3.5 w-3.5 text-brand-400" />
          <span>{lang === 'zh-CN' ? 'English' : '简体中文'}</span>
        </Button>

        <div className="h-4 w-[1px] bg-dark-700 mx-1" />

        {/* User profile & Logout */}
        <div className="flex items-center gap-2">
          <div className="flex items-center gap-2 px-2.5 py-1 rounded-lg bg-dark-800/80 border border-dark-700 text-xs text-slate-200">
            <User className="h-3.5 w-3.5 text-slate-400" />
            <span className="font-medium">{username || 'admin'}</span>
          </div>

          <Button
            variant="ghost"
            size="sm"
            onClick={onLogout}
            className="text-xs text-rose-400 hover:text-rose-300 hover:bg-rose-950/40 p-2 h-8 w-8 rounded-lg"
            title={t('auth.logout')}
          >
            <LogOut className="h-4 w-4" />
          </Button>
        </div>
      </div>
    </header>
  );
};
