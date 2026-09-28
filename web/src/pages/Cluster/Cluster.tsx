import React, { useState, useEffect } from 'react';
import { Share2, Server, HeartPulse, Clock } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getClusterNodesApi } from '../../api/client';
import { ClusterSummary } from '../../types/api';
import { Card, CardHeader, CardTitle, CardContent } from '../../components/ui/card';
import { Badge } from '../../components/ui/badge';
import { useToast } from '../../components/ui/toast';

export const Cluster: React.FC = () => {
  const { t } = useI18n();
  const { error } = useToast();
  const [cluster, setCluster] = useState<ClusterSummary | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchCluster = async () => {
    setLoading(true);
    try {
      const data = await getClusterNodesApi();
      setCluster(data);
    } catch (err: any) {
      error(err.message || t('common.error'));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchCluster();
    const interval = setInterval(fetchCluster, 3000);
    return () => clearInterval(interval);
  }, []);

  const getHealthBadge = (status: string) => {
    switch (status.toLowerCase()) {
      case 'alive':
        return <Badge variant="alive">{t('cluster.state_alive')}</Badge>;
      case 'suspect':
        return <Badge variant="suspect">{t('cluster.state_suspect')}</Badge>;
      default:
        return <Badge variant="dead">{t('cluster.state_dead')}</Badge>;
    }
  };

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <div>
          <h2 className="text-base font-semibold text-white">{t('cluster.title')}</h2>
          <p className="text-xs text-slate-400 mt-0.5">{t('cluster.subtitle')}</p>
        </div>
        <Badge variant="outline" className="font-mono text-xs">
          {(cluster?.nodes?.length || 0) + 1} Nodes Mesh
        </Badge>
      </div>

      {/* Local Self Node Card */}
      <Card className="border-brand-500/40 bg-gradient-to-r from-dark-850 to-brand-950/20">
        <CardHeader className="flex flex-row items-center justify-between pb-2">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-xl bg-brand-500/20 border border-brand-500/40 flex items-center justify-center text-brand-400">
              <Server className="h-5 w-5" />
            </div>
            <div>
              <div className="text-xs text-brand-400 font-semibold uppercase tracking-wider">
                {t('cluster.self_node')}
              </div>
              <CardTitle className="text-base font-mono text-white mt-0.5">
                {cluster?.self_node_id || 'standalone-node'}
              </CardTitle>
            </div>
          </div>
          <Badge variant="alive" className="px-3 py-1">
            Active Master
          </Badge>
        </CardHeader>
        <CardContent>
          <div className="text-xs text-slate-400 font-mono">
            Listen Address: <span className="text-slate-200">{cluster?.self_addr || 'Local'}</span>
          </div>
        </CardContent>
      </Card>

      {/* Peer Nodes Grid */}
      <div>
        <h3 className="text-sm font-semibold text-slate-300 mb-3 flex items-center gap-2">
          <Share2 className="h-4 w-4 text-cyan-400" />
          <span>{t('cluster.peer_nodes')} ({cluster?.nodes?.length || 0})</span>
        </h3>

        {!cluster?.nodes || cluster.nodes.length === 0 ? (
          <Card className="p-8 text-center text-slate-500 text-xs">
            No peer nodes discovered yet. Configure <code className="text-slate-400 font-mono">-cluster-seeds</code> or <code className="text-slate-400 font-mono">-cluster-peers</code> to join a mesh.
          </Card>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {cluster.nodes.map((peer) => (
              <Card key={peer.node_id} className="border-dark-750">
                <CardHeader className="flex flex-row items-center justify-between pb-2">
                  <div className="flex items-center gap-2.5">
                    <HeartPulse className="h-4 w-4 text-cyan-400" />
                    <CardTitle className="text-sm font-mono text-white">
                      {peer.node_id}
                    </CardTitle>
                  </div>
                  {getHealthBadge(peer.status)}
                </CardHeader>
                <CardContent className="space-y-2 text-xs">
                  <div className="p-2.5 rounded-lg bg-dark-900 border border-dark-750 font-mono text-slate-300">
                    <span className="text-slate-500 block">RPC Address:</span>
                    <span className="text-white mt-0.5 block">{peer.address}</span>
                  </div>
                  <div className="flex items-center gap-1.5 text-slate-500 font-mono text-[11px]">
                    <Clock className="h-3 w-3" />
                    <span>Last heartbeat: {new Date(peer.last_seen).toLocaleTimeString()}</span>
                  </div>
                </CardContent>
              </Card>
            ))}
          </div>
        )}
      </div>
    </div>
  );
};
