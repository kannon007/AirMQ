import React, { useState, useEffect } from 'react';
import { Database, Trash2, Eye, Copy, Check } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getRetainedApi, deleteRetainedApi } from '../../api/client';
import { RetainedSummary } from '../../types/api';
import { Table, TableHeader, TableBody, TableHead, TableRow, TableCell } from '../../components/ui/table';
import { Button } from '../../components/ui/button';
import { Badge } from '../../components/ui/badge';
import { Dialog } from '../../components/ui/dialog';
import { useToast } from '../../components/ui/toast';

export const Retained: React.FC = () => {
  const { t } = useI18n();
  const { success, error } = useToast();

  const [messages, setMessages] = useState<RetainedSummary[]>([]);
  const [loading, setLoading] = useState(false);

  // Payload Viewer Modal
  const [viewPayload, setViewPayload] = useState<RetainedSummary | null>(null);
  const [copied, setCopied] = useState(false);

  // Delete Target Modal
  const [deleteTarget, setDeleteTarget] = useState<string | null>(null);
  const [deleteLoading, setDeleteLoading] = useState(false);

  const fetchRetained = async () => {
    setLoading(true);
    try {
      const items = await getRetainedApi();
      setMessages(items);
    } catch (err: any) {
      error(err.message || t('common.error'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchRetained();
  }, []);

  const handleDeleteConfirm = async () => {
    if (!deleteTarget) return;
    setDeleteLoading(true);
    try {
      await deleteRetainedApi(deleteTarget);
      success(`Retained message on topic ${deleteTarget} deleted successfully`);
      setDeleteTarget(null);
      fetchRetained();
    } catch (err: any) {
      error(err.message || 'Failed to delete retained message');
    } finally {
      setDeleteLoading(false);
    }
  };

  const handleCopyPayload = () => {
    if (!viewPayload) return;
    navigator.clipboard.writeText(viewPayload.payload);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="space-y-4">
      {/* Header description */}
      <div className="flex items-center justify-between p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <div>
          <h2 className="text-base font-semibold text-white">{t('retained.title')}</h2>
          <p className="text-xs text-slate-400 mt-0.5">
            Retained messages are persisted in LSM-Tree (Pebble/Badger) or Memory and delivered to new subscribers.
          </p>
        </div>
        <Badge variant="outline" className="font-mono text-xs">
          Total: {messages.length} messages
        </Badge>
      </div>

      {/* Table */}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>{t('retained.topic')}</TableHead>
            <TableHead>{t('retained.qos')}</TableHead>
            <TableHead>{t('retained.size')}</TableHead>
            <TableHead>{t('retained.payload')}</TableHead>
            <TableHead className="text-right">{t('common.action')}</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {loading ? (
            <TableRow>
              <TableCell colSpan={5} className="text-center py-10 text-slate-500">
                {t('common.loading')}
              </TableCell>
            </TableRow>
          ) : messages.length === 0 ? (
            <TableRow>
              <TableCell colSpan={5} className="text-center py-10 text-slate-500">
                <div className="flex flex-col items-center justify-center gap-2">
                  <Database className="h-8 w-8 text-slate-600" />
                  <span>{t('common.nodata')}</span>
                </div>
              </TableCell>
            </TableRow>
          ) : (
            messages.map((m) => (
              <TableRow key={m.topic}>
                <TableCell className="font-mono font-semibold text-amber-400">
                  {m.topic}
                </TableCell>
                <TableCell>
                  <Badge variant="outline" className="font-mono">
                    QoS {m.qos}
                  </Badge>
                </TableCell>
                <TableCell className="font-mono text-xs text-slate-300">
                  {m.size} Bytes
                </TableCell>
                <TableCell className="font-mono text-xs text-slate-400 max-w-xs truncate">
                  {m.payload}
                </TableCell>
                <TableCell className="text-right">
                  <div className="flex items-center justify-end gap-1.5">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => setViewPayload(m)}
                      className="h-8 text-xs text-slate-300 hover:text-white"
                    >
                      <Eye className="h-3.5 w-3.5 mr-1 text-slate-400" />
                      <span>{t('retained.view_payload')}</span>
                    </Button>
                    <Button
                      variant="destructive"
                      size="sm"
                      onClick={() => setDeleteTarget(m.topic)}
                      className="h-8 text-xs gap-1"
                    >
                      <Trash2 className="h-3.5 w-3.5" />
                      <span>{t('common.delete')}</span>
                    </Button>
                  </div>
                </TableCell>
              </TableRow>
            ))
          )}
        </TableBody>
      </Table>

      {/* Payload Viewer Modal */}
      <Dialog
        open={!!viewPayload}
        onOpenChange={(o) => !o && setViewPayload(null)}
        title={t('retained.payload_modal_title')}
        description={`Topic: ${viewPayload?.topic || ''}`}
        footer={
          <div className="flex items-center justify-between w-full">
            <Button
              variant="outline"
              size="sm"
              onClick={handleCopyPayload}
              className="text-xs gap-1.5"
            >
              {copied ? <Check className="h-3.5 w-3.5 text-emerald-400" /> : <Copy className="h-3.5 w-3.5" />}
              <span>{copied ? 'Copied' : 'Copy Payload'}</span>
            </Button>
            <Button variant="secondary" size="sm" onClick={() => setViewPayload(null)}>
              {t('common.close')}
            </Button>
          </div>
        }
      >
        <div className="rounded-lg bg-dark-950 p-4 border border-dark-750 font-mono text-xs text-slate-200 overflow-x-auto max-h-80 whitespace-pre-wrap">
          {viewPayload?.payload}
        </div>
      </Dialog>

      {/* Delete Retained Modal */}
      <Dialog
        open={!!deleteTarget}
        onOpenChange={(o) => !o && setDeleteTarget(null)}
        title={t('retained.delete_confirm_title')}
        description={t('retained.delete_confirm_desc')}
        footer={
          <>
            <Button variant="outline" onClick={() => setDeleteTarget(null)}>
              {t('common.cancel')}
            </Button>
            <Button variant="destructive" onClick={handleDeleteConfirm} disabled={deleteLoading}>
              {deleteLoading ? 'Deleting...' : t('common.delete')}
            </Button>
          </>
        }
      >
        <div className="p-3 rounded-lg bg-dark-850 border border-dark-750 text-sm font-mono text-amber-400">
          Topic: {deleteTarget}
        </div>
      </Dialog>
    </div>
  );
};
