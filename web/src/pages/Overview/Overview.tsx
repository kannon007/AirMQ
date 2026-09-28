import React, { useState, useEffect, useRef } from 'react';
import {
  Radio,
  Layers,
  Database,
  Share2,
  Cpu,
  Server,
  TrendingUp,
  Activity,
  HardDrive,
  Clock,
} from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { getOverviewApi } from '../../api/client';
import { OverviewStats } from '../../types/api';
import { Card, CardHeader, CardTitle, CardContent } from '../../components/ui/card';
import { Badge } from '../../components/ui/badge';
import { QpsChart, DataPoint } from '../../components/charts/QpsChart';

export const Overview: React.FC = () => {
  const { t } = useI18n();
  const [stats, setStats] = useState<OverviewStats | null>(null);
  const [chartData, setChartData] = useState<DataPoint[]>([]);
  const prevMetricsRef = useRef<{ inMsg: number; outMsg: number; time: number } | null>(null);

  const fetchStats = async () => {
    try {
      const data = await getOverviewApi();
      setStats(data);

      const now = Date.now();
      const timeStr = new Date().toLocaleTimeString();

      if (prevMetricsRef.current) {
        const deltaSec = (now - prevMetricsRef.current.time) / 1000;
        if (deltaSec > 0) {
          const inRate = Math.max(0, Math.round((data.total_msg_received - prevMetricsRef.current.inMsg) / deltaSec));
          const outRate = Math.max(0, Math.round((data.total_msg_sent - prevMetricsRef.current.outMsg) / deltaSec));

          setChartData((prev) => {
            const next = [...prev, { time: timeStr, inbound: inRate, outbound: outRate }];
            return next.slice(-30); // 30 points window
          });
        }
      } else {
        setChartData([{ time: timeStr, inbound: 0, outbound: 0 }]);
      }

      prevMetricsRef.current = {
        inMsg: data.total_msg_received,
        outMsg: data.total_msg_sent,
        time: now,
      };
    } catch (err) {
      console.error('Failed to fetch overview stats:', err);
    }
  };

  useEffect(() => {
    fetchStats();
    const interval = setInterval(() => {
      if (!document.hidden) {
        fetchStats();
      }
    }, 2000);
    return () => clearInterval(interval);
  }, []);

  const formatUptime = (seconds: number) => {
    const d = Math.floor(seconds / (3600 * 24));
    const h = Math.floor((seconds % (3600 * 24)) / 3600);
    const m = Math.floor((seconds % 3600) / 60);
    const s = Math.floor(seconds % 60);
    if (d > 0) return `${d}d ${h}h ${m}m`;
    if (h > 0) return `${h}h ${m}m ${s}s`;
    return `${m}m ${s}s`;
  };

  const formatBytes = (bytes: number) => {
    if (!bytes || bytes <= 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <div className="space-y-6">
      {/* Top 4 Metric KPI Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        {/* Active Connections */}
        <Card className="border-dark-750 bg-gradient-to-br from-dark-850 to-dark-900">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium text-slate-400">
              {t('overview.active_conns')}
            </CardTitle>
            <div className="w-8 h-8 rounded-lg bg-emerald-500/15 border border-emerald-500/30 flex items-center justify-center text-emerald-400">
              <Radio className="h-4 w-4" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-mono text-white">
              {stats?.active_connections ?? 0}
            </div>
            <div className="flex flex-wrap items-center gap-1.5 mt-2">
              <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-blue-500/15 text-blue-400 border border-blue-500/30">
                TCP: {stats?.tcp_connections ?? 0}
              </span>
              <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-purple-500/15 text-purple-400 border border-purple-500/30">
                TLS: {stats?.tls_connections ?? 0}
              </span>
              <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-amber-500/15 text-amber-400 border border-amber-500/30">
                WS: {stats?.ws_connections ?? 0}
              </span>
              <span className="text-[10px] font-mono px-1.5 py-0.5 rounded bg-emerald-500/20 text-emerald-300 border border-emerald-500/40">
                QUIC: {stats?.quic_connections ?? 0}
              </span>
            </div>
          </CardContent>
        </Card>

        {/* Subscriptions */}
        <Card className="border-dark-750 bg-gradient-to-br from-dark-850 to-dark-900">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium text-slate-400">
              {t('overview.total_subs')}
            </CardTitle>
            <div className="w-8 h-8 rounded-lg bg-cyan-500/15 border border-cyan-500/30 flex items-center justify-center text-cyan-400">
              <Layers className="h-4 w-4" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-mono text-white">
              {stats?.subscriptions_count ?? 0}
            </div>
            <p className="text-xs text-slate-400 mt-2 flex items-center gap-1">
              <span className="text-cyan-400 font-medium">ART Trie</span> Tree Cache
            </p>
          </CardContent>
        </Card>

        {/* Retained Messages */}
        <Card className="border-dark-750 bg-gradient-to-br from-dark-850 to-dark-900">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium text-slate-400">
              {t('overview.retained_msgs')}
            </CardTitle>
            <div className="w-8 h-8 rounded-lg bg-amber-500/15 border border-amber-500/30 flex items-center justify-center text-amber-400">
              <Database className="h-4 w-4" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-mono text-white">
              {stats?.retained_count ?? 0}
            </div>
            <p className="text-xs text-slate-400 mt-2 flex items-center gap-1">
              <span className="text-amber-400 font-medium">Persisted</span> in Engine
            </p>
          </CardContent>
        </Card>

        {/* Cluster Nodes */}
        <Card className="border-dark-750 bg-gradient-to-br from-dark-850 to-dark-900">
          <CardHeader className="flex flex-row items-center justify-between pb-2">
            <CardTitle className="text-xs font-medium text-slate-400">
              {t('overview.cluster_nodes')}
            </CardTitle>
            <div className="w-8 h-8 rounded-lg bg-purple-500/15 border border-purple-500/30 flex items-center justify-center text-purple-400">
              <Share2 className="h-4 w-4" />
            </div>
          </CardHeader>
          <CardContent>
            <div className="text-2xl font-bold font-mono text-white">
              {stats?.cluster_nodes_count ?? 1}
            </div>
            <p className="text-xs text-slate-400 mt-2 flex items-center gap-1">
              <span className="text-purple-400 font-medium">PEX</span> Mesh Mesh Peers
            </p>
          </CardContent>
        </Card>
      </div>

      {/* Realtime Throughput Chart Card */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div className="flex items-center gap-2">
            <TrendingUp className="h-5 w-5 text-brand-400" />
            <CardTitle>{t('overview.realtime_throughput')}</CardTitle>
          </div>
          <Badge variant="outline" className="font-mono text-xs">
            Poll 2s / Window 60s
          </Badge>
        </CardHeader>
        <CardContent>
          <QpsChart data={chartData} height={260} />
        </CardContent>
      </Card>

      {/* Node Hardware Specifications & Cumulative Counters */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Node Specs */}
        <Card>
          <CardHeader className="flex flex-row items-center gap-2 pb-3">
            <Server className="h-5 w-5 text-brand-400" />
            <CardTitle>{t('overview.node_specs')}</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.node_name')}</div>
                <div className="font-mono text-white font-medium mt-1 truncate">
                  {stats?.node_name || 'standalone-node'}
                </div>
              </div>
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.version')}</div>
                <div className="font-mono text-brand-400 font-medium mt-1">
                  v{stats?.version || '1.0.0'}
                </div>
              </div>
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.uptime')}</div>
                <div className="font-mono text-white font-medium mt-1 flex items-center gap-1.5">
                  <Clock className="h-3.5 w-3.5 text-slate-400" />
                  <span>{stats ? formatUptime(stats.uptime_seconds) : '-'}</span>
                </div>
              </div>
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.os_arch')}</div>
                <div className="font-mono text-white font-medium mt-1">
                  {stats?.os}/{stats?.arch}
                </div>
              </div>
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.go_version')}</div>
                <div className="font-mono text-white font-medium mt-1">
                  {stats?.go_version || '-'}
                </div>
              </div>
              <div className="p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="text-xs text-slate-400">{t('overview.num_cpu')}</div>
                <div className="font-mono text-white font-medium mt-1 flex items-center gap-1">
                  <Cpu className="h-3.5 w-3.5 text-slate-400" />
                  <span>{stats?.num_cpu ?? 0} Cores</span>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Runtime Performance & Cumulative Stats */}
        <Card>
          <CardHeader className="flex flex-row items-center gap-2 pb-3">
            <Activity className="h-5 w-5 text-brand-400" />
            <CardTitle>Runtime Telemetry & Performance</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4 text-sm">
              <div className="flex items-center justify-between p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="flex items-center gap-2 text-slate-300">
                  <Cpu className="h-4 w-4 text-brand-400" />
                  <span>{t('overview.goroutines')}</span>
                </div>
                <span className="font-mono font-bold text-white text-base">
                  {stats?.goroutines ?? 0}
                </span>
              </div>

              <div className="flex items-center justify-between p-3 rounded-lg bg-dark-900/90 border border-dark-750">
                <div className="flex items-center gap-2 text-slate-300">
                  <HardDrive className="h-4 w-4 text-cyan-400" />
                  <span>{t('overview.mem_alloc')}</span>
                </div>
                <div className="text-right font-mono">
                  <span className="font-bold text-white text-base">
                    {stats?.memory_alloc_mb.toFixed(2) ?? '0.00'} MB
                  </span>
                  <span className="text-xs text-slate-400 ml-2">
                    (Sys: {stats?.memory_sys_mb.toFixed(2) ?? '0.00'} MB)
                  </span>
                </div>
              </div>

              <div className="grid grid-cols-2 gap-3 pt-1">
                <div className="p-3 rounded-lg bg-dark-900/70 border border-dark-750">
                  <div className="text-xs text-slate-400">{t('overview.bytes_in')}</div>
                  <div className="font-mono font-semibold text-emerald-400 text-sm mt-1">
                    {stats ? formatBytes(stats.total_bytes_in) : '0 B'}
                  </div>
                </div>

                <div className="p-3 rounded-lg bg-dark-900/70 border border-dark-750">
                  <div className="text-xs text-slate-400">{t('overview.bytes_out')}</div>
                  <div className="font-mono font-semibold text-cyan-400 text-sm mt-1">
                    {stats ? formatBytes(stats.total_bytes_out) : '0 B'}
                  </div>
                </div>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
};
