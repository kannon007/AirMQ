import React, { useState, useEffect } from 'react';
import { Network, Radio, ShieldCheck, Globe, Zap } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getListenersApi } from '../../api/client';
import { ListenerSummary } from '../../types/api';
import { Card, CardHeader, CardTitle, CardContent } from '../../components/ui/card';
import { Badge } from '../../components/ui/badge';
import { useToast } from '../../components/ui/toast';

export const Listeners: React.FC = () => {
  const { t } = useI18n();
  const { error } = useToast();
  const [listeners, setListeners] = useState<ListenerSummary[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchListeners = async () => {
    setLoading(true);
    try {
      const items = await getListenersApi();
      setListeners(items);
    } catch (err: any) {
      error(err.message || t('common.error'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchListeners();
    const interval = setInterval(fetchListeners, 3000);
    return () => clearInterval(interval);
  }, []);

  const getListenerIcon = (name: string) => {
    switch (name.toLowerCase()) {
      case 'tls':
        return <ShieldCheck className="h-6 w-6 text-purple-400" />;
      case 'ws':
      case 'websocket':
        return <Globe className="h-6 w-6 text-amber-400" />;
      case 'quic':
        return <Zap className="h-6 w-6 text-emerald-400" />;
      default:
        return <Radio className="h-6 w-6 text-blue-400" />;
    }
  };

  return (
    <div className="space-y-6">
      {/* Title */}
      <div className="flex items-center justify-between p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <div>
          <h2 className="text-base font-semibold text-white">{t('listeners.title')}</h2>
          <p className="text-xs text-slate-400 mt-0.5">{t('listeners.subtitle')}</p>
        </div>
        <Badge variant="outline" className="font-mono text-xs">
          {listeners.length} Listeners Online
        </Badge>
      </div>

      {/* Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {listeners.map((l) => (
          <Card key={`${l.name}-${l.address}`} className="relative overflow-hidden border-dark-750">
            <CardHeader className="flex flex-row items-center justify-between pb-3">
              <div className="flex items-center gap-3">
                <div className="w-12 h-12 rounded-xl bg-dark-800 border border-dark-700 flex items-center justify-center shadow-md">
                  {getListenerIcon(l.name)}
                </div>
                <div>
                  <CardTitle className="text-base uppercase tracking-wide flex items-center gap-2">
                    <span>{l.name}</span>
                    <Badge variant={l.name.toLowerCase() as any} className="text-[10px]">
                      {l.protocol.toUpperCase()}
                    </Badge>
                  </CardTitle>
                  <p className="font-mono text-xs text-slate-300 mt-0.5">{l.address}</p>
                </div>
              </div>

              <div className="flex items-center gap-1.5 px-2 py-1 rounded-full bg-emerald-500/10 border border-emerald-500/30 text-emerald-400 text-xs">
                <span className="w-1.5 h-1.5 rounded-full bg-emerald-400 animate-pulse" />
                <span className="capitalize">{l.status}</span>
              </div>
            </CardHeader>

            <CardContent>
              <div className="grid grid-cols-2 gap-3 pt-3 border-t border-dark-800">
                <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                  <span className="text-xs text-slate-400 block">{t('listeners.conns')}</span>
                  <span className="text-xl font-bold font-mono text-white mt-1 block">
                    {l.active_conns}
                  </span>
                </div>

                <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                  <span className="text-xs text-slate-400 block">{t('listeners.protocol')}</span>
                  <span className="text-sm font-mono text-slate-200 mt-1 block uppercase font-medium">
                    {l.protocol} Socket
                  </span>
                </div>
              </div>
            </CardContent>
          </Card>
        ))}
      </div>
    </div>
  );
};
