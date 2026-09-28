import React, { useState, useEffect } from 'react';
import {
  Search,
  Radio,
  Trash2,
  Eye,
  ShieldCheck,
  Zap,
  Clock,
  Layers,
  CheckCircle2,
  XCircle,
} from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getClientsApi, getClientDetailApi, kickClientApi } from '../../api/client';
import { ClientSummary, ClientDetail } from '../../types/api';
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from '../../components/ui/table';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import { Dialog } from '../../components/ui/dialog';
import { Sheet } from '../../components/ui/sheet';
import { useToast } from '../../components/ui/toast';

export const Clients: React.FC = () => {
  const { t } = useI18n();
  const { success, error } = useToast();

  const [clients, setClients] = useState<ClientSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [query, setQuery] = useState('');
  const [transportFilter, setTransportFilter] = useState('all');
  const [loading, setLoading] = useState(false);

  // Inspector Drawer State
  const [detailClient, setDetailClient] = useState<ClientDetail | null>(null);
  const [drawerOpen, setDrawerOpen] = useState(false);

  // Kick Modal State
  const [kickTarget, setKickTarget] = useState<string | null>(null);
  const [kickLoading, setKickLoading] = useState(false);

  const fetchClients = async () => {
    setLoading(true);
    try {
      const q = transportFilter !== 'all' ? transportFilter : query;
      const res = await getClientsApi(page, 20, q);
      setClients(res.items || []);
      setTotal(res.total || 0);
    } catch (err: any) {
      error(err.message || t('common.error'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchClients();
  }, [page, transportFilter]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    fetchClients();
  };

  const handleOpenDetail = async (clientId: string) => {
    try {
      const data = await getClientDetailApi(clientId);
      setDetailClient(data);
      setDrawerOpen(true);
    } catch (err: any) {
      error(err.message || 'Failed to fetch client details');
    }
  };

  const handleKickConfirm = async () => {
    if (!kickTarget) return;
    setKickLoading(true);
    try {
      await kickClientApi(kickTarget);
      success(`Client ${kickTarget} disconnected successfully`);
      setKickTarget(null);
      fetchClients();
    } catch (err: any) {
      error(err.message || 'Failed to kick client');
    } finally {
      setKickLoading(false);
    }
  };

  const getTransportBadge = (transport: string) => {
    switch (transport.toLowerCase()) {
      case 'tls':
        return <Badge variant="tls">TLS 8883</Badge>;
      case 'ws':
      case 'websocket':
        return <Badge variant="ws">WS 8083</Badge>;
      case 'quic':
        return <Badge variant="quic">QUIC 14567</Badge>;
      default:
        return <Badge variant="tcp">TCP 1883</Badge>;
    }
  };

  return (
    <div className="space-y-4">
      {/* Header filter bar */}
      <div className="flex flex-col sm:flex-row items-stretch sm:items-center justify-between gap-3 p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <form onSubmit={handleSearch} className="flex items-center gap-2 flex-1 max-w-md">
          <div className="relative w-full">
            <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <Input
              type="text"
              placeholder={t('clients.search_placeholder')}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button type="submit" variant="secondary" size="sm">
            {t('common.search')}
          </Button>
        </form>

        {/* Transport pills filter */}
        <div className="flex items-center gap-1.5 p-1 rounded-lg bg-dark-800 border border-dark-700 text-xs">
          {['all', 'tcp', 'tls', 'ws', 'quic'].map((tp) => (
            <button
              key={tp}
              onClick={() => {
                setTransportFilter(tp);
                setPage(1);
              }}
              className={`px-2.5 py-1 rounded-md uppercase font-mono font-medium transition-all ${
                transportFilter === tp
                  ? 'bg-brand-500 text-white shadow-sm'
                  : 'text-slate-400 hover:text-white hover:bg-dark-700'
              }`}
            >
              {tp}
            </button>
          ))}
        </div>
      </div>

      {/* Data Table */}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{t('clients.client_id')}</TableHead>
            <TableHead>{t('clients.username')}</TableHead>
            <TableHead>{t('clients.ip_address')}</TableHead>
            <TableHead>{t('clients.protocol')}</TableHead>
            <TableHead>{t('clients.transport')}</TableHead>
            <TableHead>{t('clients.keepalive')}</TableHead>
            <TableHead>{t('clients.subs_count')}</TableHead>
            <TableHead className="text-right">{t('common.action')}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <TableRow>
              <TableCell colSpan={8} className="text-center py-10 text-slate-500">
                {t('common.loading')}
              </TableCell>
            </TableRow>
          ) : clients.length === 0 ? (
            <TableRow>
              <TableCell colSpan={8} className="text-center py-10 text-slate-500">
                <div className="flex flex-col items-center justify-center gap-2">
                  <Radio className="h-8 w-8 text-slate-600" />
                  <span>{t('common.nodata')}</span>
                </div>
              </TableCell>
            </TableRow>
          ) : (
            clients.map((c) => (
              <TableRow key={c.client_id}>
                <TableCell className="font-mono font-semibold text-white">
                  {c.client_id}
                </TableCell>
                <TableCell className="text-slate-300">
                  {c.username || <span className="text-slate-500 italic">anonymous</span>}
                </TableCell>
                <TableCell className="font-mono text-xs text-slate-300">
                  {c.ip_address}
                </TableCell>
                <TableCell>
                  <span className="text-xs font-mono px-2 py-0.5 rounded bg-dark-800 border border-dark-700 text-slate-300">
                    {c.protocol}
                  </span>
                </TableCell>
                <TableCell>{getTransportBadge(c.transport)}</TableCell>
                <TableCell className="font-mono text-xs text-slate-300">
                  {c.keep_alive}s
                </TableCell>
                <TableCell>
                  <span className="inline-flex items-center gap-1 font-mono text-xs font-semibold px-2 py-0.5 rounded bg-dark-800 border border-dark-700 text-cyan-400">
                    <Layers className="h-3 w-3" />
                    {c.subscriptions_count}
                  </span>
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex items-center justify-end gap-1.5">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => handleOpenDetail(c.client_id)}
                      className="h-8 text-xs text-slate-300 hover:text-white"
                      title={t('common.details')}
                    >
                      <Eye className="h-3.5 w-3.5 mr-1 text-slate-400" />
                      <span>{t('common.details')}</span>
                    </Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => setKickTarget(c.client_id)}
                      className="h-8 text-xs gap-1"
                      title={t('clients.kick')}
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                      <span>{t('clients.kick')}</span>
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {/* Pagination Footer */}
      <div className="flex items-center justify-between text-xs text-slate-400 px-2">
        <span>Total: {total} clients</span>
        <div className="flex items-center gap-2">
          <Button
            variant="outline"
            size="sm"
            disabled={page <= 1}
            onClick={() => setPage((p) => p - 1)}
          >
            Previous
          </Button>
          <span className="font-mono font-medium text-white px-2">
            Page {page} of {Math.max(1, Math.ceil(total / 20))}
          </span>
          <Button
            variant="outline"
            size="sm"
            disabled={page >= Math.ceil(total / 20)}
            onClick={() => setPage((p) => p + 1)}
          >
            Next
          </Button>
        </div>
      </div>

      {/* Client Detail Drawer (Sheet) */}
      <Sheet
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        title={t('clients.drawer_title')}
        description={`ClientID: ${detailClient?.client_id || ''}`}
        width="max-w-lg"
      >
        {detailClient && (
          <div className="space-y-5 text-sm">
            {/* Quick overview block */}
            <div className="grid grid-cols-2 gap-3 p-4 rounded-xl bg-dark-850 border border-dark-750">
              <div>
                <span className="text-xs text-slate-400 block">{t('clients.transport')}</span>
                <div className="mt-1">{getTransportBadge(detailClient.transport)}</div>
              </div>
              <div>
                <span className="text-xs text-slate-400 block">{t('clients.protocol')}</span>
                <span className="font-mono text-white text-xs font-semibold mt-1 block">
                  {detailClient.protocol}
                </span>
              </div>
              <div>
                <span className="text-xs text-slate-400 block">{t('clients.ip_address')}</span>
                <span className="font-mono text-slate-200 text-xs mt-1 block">
                  {detailClient.ip_address}
                </span>
              </div>
              <div>
                <span className="text-xs text-slate-400 block">{t('clients.clean_session')}</span>
                <span className="text-xs font-medium mt-1 flex items-center gap-1 text-slate-300">
                  {detailClient.clean_session ? (
                    <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400" />
                  ) : (
                    <XCircle className="h-3.5 w-3.5 text-amber-400" />
                  )}
                  {detailClient.clean_session ? 'True' : 'False (Persistent)'}
                </span>
              </div>
            </div>

            {/* Security / TLS info */}
            {detailClient.is_tls && (
              <div className="p-3.5 rounded-xl bg-purple-950/20 border border-purple-500/30 text-xs space-y-1">
                <div className="flex items-center gap-1.5 text-purple-400 font-semibold">
                  <ShieldCheck className="h-4 w-4" />
                  <span>mTLS Authentication</span>
                </div>
                {detailClient.client_cert_cn && (
                  <p className="text-slate-300">
                    Certificate CN: <span className="font-mono text-white">{detailClient.client_cert_cn}</span>
                  </p>
                )}
              </div>
            )}

            {/* Will message */}
            {detailClient.will_topic && (
              <div className="p-3.5 rounded-xl bg-dark-850 border border-dark-750 text-xs">
                <span className="text-slate-400 block">{t('clients.will_topic')}:</span>
                <span className="font-mono text-amber-400 mt-1 block break-all">
                  {detailClient.will_topic}
                </span>
              </div>
            )}

            {/* Subscriptions list */}
            <div>
              <h4 className="text-xs font-semibold uppercase tracking-wider text-slate-400 mb-2 flex items-center justify-between">
                <span>{t('clients.active_subs')}</span>
                <span className="font-mono text-cyan-400">
                  ({detailClient.subscriptions?.length || 0})
                </span>
              </h4>

              <div className="max-h-60 overflow-y-auto space-y-1.5 pr-1">
                {!detailClient.subscriptions || detailClient.subscriptions.length === 0 ? (
                  <div className="p-4 rounded-lg bg-dark-850 border border-dark-750 text-center text-xs text-slate-500">
                    {t('clients.no_subs')}
                  </div>
                ) : (
                  detailClient.subscriptions.map((sub, idx) => (
                    <div
                      key={idx}
                      className="p-2.5 rounded-lg bg-dark-850 border border-dark-750 flex items-center justify-between text-xs font-mono"
                    >
                      <span className="text-white truncate">{sub}</span>
                      <Badge variant="outline" className="text-[10px]">
                        Active
                      </Badge>
                    </div>
                  ))
                )}
              </div>
            </div>
          </div>
        )}
      </Sheet>

      {/* Kick Confirmation Modal */}
      <Dialog
        open={!!kickTarget}
        onOpenChange={(o) => !o && setKickTarget(null)}
        title={t('clients.kick_confirm_title')}
        description={t('clients.kick_confirm_desc')}
        footer={
          <>
            <Button variant="outline" onClick={() => setKickTarget(null)}>
              {t('common.cancel')}
            </Button>
            <Button variant="destructive" onClick={handleKickConfirm} disabled={kickLoading}>
              {kickLoading ? 'Disconnecting...' : t('clients.kick')}
            </Button>
          </>
        }
      >
        <p className="text-sm font-mono text-slate-300">
          Target Client ID: <span className="font-bold text-rose-400">{kickTarget}</span>
        </p>
      </Dialog>
    </div>
  );
};
