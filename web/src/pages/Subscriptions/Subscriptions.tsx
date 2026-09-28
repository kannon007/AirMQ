import React, { useState, useEffect } from 'react';
import { Search, Layers, Trash2 } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getSubscriptionsApi, unsubscribeClientApi } from '../../api/client';
import { SubscriptionSummary } from '../../types/api';
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from '../../components/ui/table';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Badge } from '../../components/ui/badge';
import { Dialog } from '../../components/ui/dialog';
import { useToast } from '../../components/ui/toast';

export const Subscriptions: React.FC = () => {
  const { t } = useI18n();
  const { success, error } = useToast();

  const [subs, setSubs] = useState<SubscriptionSummary[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [query, setQuery] = useState('');
  const [loading, setLoading] = useState(false);

  // Unsubscribe Target State
  const [unsubTarget, setUnsubTarget] = useState<{ clientId: string; topic: string } | null>(null);
  const [unsubLoading, setUnsubLoading] = useState(false);

  const fetchSubscriptions = async () => {
    setLoading(true);
    try {
      const res = await getSubscriptionsApi(page, 20, query);
      setSubs(res.items || []);
      setTotal(res.total || 0);
    } catch (err: any) {
      error(err.message || t('common.error'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchSubscriptions();
  }, [page]);

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    setPage(1);
    fetchSubscriptions();
  };

  const handleUnsubscribeConfirm = async () => {
    if (!unsubTarget) return;
    setUnsubLoading(true);
    try {
      await unsubscribeClientApi(unsubTarget.clientId, unsubTarget.topic);
      success(`Unsubscribed ${unsubTarget.clientId} from ${unsubTarget.topic}`);
      setUnsubTarget(null);
      fetchSubscriptions();
    } catch (err: any) {
      error(err.message || 'Failed to unsubscribe');
    } finally {
      setUnsubLoading(false);
    }
  };

  return (
    <div className="space-y-4">
      {/* Search Header */}
      <div className="p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <form onSubmit={handleSearch} className="flex items-center gap-2 max-w-md">
          <div className="relative w-full">
            <Search className="h-4 w-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
            <Input
              type="text"
              placeholder={t('subscriptions.search_placeholder')}
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="pl-9"
            />
          </div>
          <Button type="submit" variant="secondary" size="sm">
            {t('common.search')}
          </Button>
        </form>
      </div>

      {/* Table */}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{t('subscriptions.topic')}</TableHead>
            <TableHead>{t('subscriptions.client_id')}</TableHead>
            <TableHead>{t('subscriptions.qos')}</TableHead>
            <TableHead className="text-right">{t('common.action')}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <TableRow>
              <TableCell colSpan={4} className="text-center py-10 text-slate-500">
                {t('common.loading')}
              </TableCell>
            </TableRow>
          ) : subs.length === 0 ? (
            <TableRow>
              <TableCell colSpan={4} className="text-center py-10 text-slate-500">
                <div className="flex flex-col items-center justify-center gap-2">
                  <Layers className="h-8 w-8 text-slate-600" />
                  <span>{t('common.nodata')}</span>
                </div>
              </TableCell>
            </TableRow>
          ) : (
            subs.map((s, idx) => (
              <TableRow key={`${s.client_id}-${s.topic}-${idx}`}>
                <TableCell className="font-mono font-semibold text-cyan-400">
                  {s.topic}
                </TableCell>
                <TableCell className="font-mono text-slate-200">
                  {s.client_id}
                </TableCell>
                <TableCell>
                  <Badge variant="outline" className="font-mono">
                    QoS {s.qos}
                  </Badge>
                </TableCell>
                <TableCell className="text-right">
                  <Button
                    variant="destructive"
                    size="sm"
                    onClick={() => setUnsubTarget({ clientId: s.client_id, topic: s.topic })}
                    className="h-8 text-xs gap-1"
                  >
                    <Trash2 className="h-3.5 w-3.5" />
                    <span>{t('subscriptions.unsubscribe')}</span>
                  </Button>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {/* Pagination Footer */}
      <div className="flex items-center justify-between text-xs text-slate-400 px-2">
        <span>Total: {total} subscriptions</span>
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

      {/* Unsubscribe Confirm Dialog */}
      <Dialog
        open={!!unsubTarget}
        onOpenChange={(o) => !o && setUnsubTarget(null)}
        title={t('subscriptions.unsub_confirm_title')}
        description={t('subscriptions.unsub_confirm_desc')}
        footer={
          <>
            <Button variant="outline" onClick={() => setUnsubTarget(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              variant="destructive"
              onClick={handleUnsubscribeConfirm}
              disabled={unsubLoading}
            >
              {unsubLoading ? 'Unsubscribing...' : t('subscriptions.unsubscribe')}
            </Button>
          </>
        }
      >
        {unsubTarget && (
          <div className="space-y-2 text-sm font-mono p-3 rounded-lg bg-dark-850 border border-dark-750">
            <div>Client: <span className="text-white">{unsubTarget.clientId}</span></div>
            <div>Topic: <span className="text-cyan-400">{unsubTarget.topic}</span></div>
          </div>
        )}
      </Dialog>
    </div>
  );
};
