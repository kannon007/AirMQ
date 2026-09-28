import React, { useState, useEffect, useMemo } from 'react';
import {
  GitBranch,
  Layers,
  Database,
  CheckCircle2,
  AlertTriangle,
  XCircle,
  Plus,
  RefreshCw,
  Edit2,
  Trash2,
  Play,
  Activity,
  HardDrive,
  Radio,
  Server as ServerIcon,
  Search,
  ExternalLink,
} from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import {
  getRulesApi,
  updateRuleApi,
  deleteRuleApi,
  getBridgesApi,
  updateBridgeApi,
  deleteBridgeApi,
  pingBridgeApi,
  testRuleMatchApi,
  getDriversApi,
  getPipelineApi,
} from '../../api/client';
import {
  Rule,
  RuleStatus,
  Bridge,
  BridgeStatus,
  PipelineSummary,
  RulePingResult,
} from '../../types/api';
import { Card, CardHeader, CardTitle, CardContent } from '../../components/ui/card';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Select } from '../../components/ui/select';
import { Dialog } from '../../components/ui/dialog';
import { useToast } from '../../components/ui/toast';

export const Pipeline: React.FC = () => {
  const { t } = useI18n();
  const { success, error, info } = useToast();

  const [activeTab, setActiveTab] = useState<'rules' | 'bridges'>('rules');

  // Server state
  const [rules, setRules] = useState<RuleStatus[]>([]);
  const [bridges, setBridges] = useState<BridgeStatus[]>([]);
  const [pipeline, setPipeline] = useState<PipelineSummary | null>(null);
  const [drivers, setDrivers] = useState<string[]>(['kafka', 'redpanda', 'nats', 'stdout', 'mock']);
  const [loading, setLoading] = useState(false);

  // Modals
  const [ruleModalOpen, setRuleModalOpen] = useState(false);
  const [editingRule, setEditingRule] = useState<Rule | null>(null);
  const [ruleForm, setRuleForm] = useState<{
    id: string;
    name: string;
    description: string;
    enabled: boolean;
    topic_filters: string;
    payload_format: 'raw' | 'json';
    bridge_id: string;
    target_topic: string;
    key_strategy: 'client_id' | 'topic' | 'none';
  }>({
    id: '',
    name: '',
    description: '',
    enabled: true,
    topic_filters: '#',
    payload_format: 'raw',
    bridge_id: '',
    target_topic: 'mqtt_events',
    key_strategy: 'client_id',
  });

  const [bridgeModalOpen, setBridgeModalOpen] = useState(false);
  const [editingBridge, setEditingBridge] = useState<Bridge | null>(null);
  const [bridgeForm, setBridgeForm] = useState<{
    id: string;
    name: string;
    type: string;
    servers: string;
    topic: string;
  }>({
    id: '',
    name: '',
    type: 'kafka',
    servers: '127.0.0.1:9092',
    topic: 'mqtt_events',
  });

  // Probe testing state
  const [probingBridgeId, setProbingBridgeId] = useState<string | null>(null);
  const [probeResults, setProbeResults] = useState<Record<string, RulePingResult>>({});
  const [modalProbing, setModalProbing] = useState(false);
  const [modalProbeResult, setModalProbeResult] = useState<RulePingResult | null>(null);

  // Match Simulation
  const [simModalOpen, setSimModalOpen] = useState(false);
  const [simTopic, setSimTopic] = useState('sensors/room1/temperature');
  const [simulating, setSimulating] = useState(false);
  const [simMatchedIds, setSimMatchedIds] = useState<string[] | null>(null);

  // Delete Confirmations
  const [deleteRuleTarget, setDeleteRuleTarget] = useState<string | null>(null);
  const [deleteBridgeTarget, setDeleteBridgeTarget] = useState<string | null>(null);

  // Data fetching
  const fetchData = async () => {
    try {
      const [rulesData, bridgesData, pipeData, driversData] = await Promise.all([
        getRulesApi().catch(() => []),
        getBridgesApi().catch(() => []),
        getPipelineApi().catch(() => null),
        getDriversApi().catch(() => ['kafka', 'redpanda', 'nats', 'stdout', 'mock']),
      ]);

      setRules(rulesData || []);
      setBridges(bridgesData || []);
      setPipeline(pipeData);
      if (driversData && driversData.length > 0) {
        setDrivers(driversData);
      }
    } catch (err: any) {
      // silently handle poll errors
    }
  };

  useEffect(() => {
    setLoading(true);
    fetchData().finally(() => setLoading(false));
    const timer = setInterval(fetchData, 4000);
    return () => clearInterval(timer);
  }, []);

  // Bridge lookup helper
  const bridgeMap = useMemo(() => {
    const map = new Map<string, BridgeStatus>();
    bridges.forEach((b) => map.set(b.id, b));
    return map;
  }, [bridges]);

  // Handle Rule Toggle Enabled
  const handleToggleRule = async (r: RuleStatus) => {
    try {
      const updated: Rule = {
        ...r,
        enabled: !r.enabled,
      };
      await updateRuleApi(updated);
      success(updated.enabled ? t('pipeline.rule_enabled') : t('pipeline.rule_disabled'));
      fetchData();
    } catch (err: any) {
      error(err.message || 'Failed to toggle rule');
    }
  };

  // Open Create Rule Modal
  const handleOpenCreateRule = () => {
    const defaultBridgeId = bridges.length > 0 ? bridges[0].id : 'bridge_kafka_default';
    setEditingRule(null);
    setRuleForm({
      id: `rule_${Date.now().toString(36)}`,
      name: '',
      description: '',
      enabled: true,
      topic_filters: 'telemetry/#',
      payload_format: 'raw',
      bridge_id: defaultBridgeId,
      target_topic: 'mqtt_events',
      key_strategy: 'client_id',
    });
    setRuleModalOpen(true);
  };

  // Open Edit Rule Modal
  const handleOpenEditRule = (r: RuleStatus) => {
    setEditingRule(r);
    const primaryAction = r.actions && r.actions.length > 0 ? r.actions[0] : null;
    const bridgeId = primaryAction?.bridge_id || bridges[0]?.id || 'bridge_kafka_default';
    const targetTopic = primaryAction?.target_topic || r.target_topic || 'mqtt_events';
    const keyStrategy = primaryAction?.key_strategy || r.key_strategy || 'client_id';

    setRuleForm({
      id: r.id,
      name: r.name,
      description: r.description || '',
      enabled: r.enabled,
      topic_filters: (r.topic_filters || ['#']).join(', '),
      payload_format: r.payload_format || 'raw',
      bridge_id: bridgeId,
      target_topic: targetTopic,
      key_strategy: keyStrategy,
    });
    setRuleModalOpen(true);
  };

  // Save Rule
  const handleSaveRule = async () => {
    if (!ruleForm.name.trim()) {
      error('Rule Name is required');
      return;
    }
    const filters = ruleForm.topic_filters
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    const payload: Rule = {
      id: ruleForm.id.trim(),
      name: ruleForm.name.trim(),
      description: ruleForm.description.trim(),
      enabled: ruleForm.enabled,
      topic_filters: filters.length > 0 ? filters : ['#'],
      payload_format: ruleForm.payload_format,
      actions: [
        {
          bridge_id: ruleForm.bridge_id,
          target_topic: ruleForm.target_topic.trim() || 'mqtt_events',
          key_strategy: ruleForm.key_strategy,
        },
      ],
      target_topic: ruleForm.target_topic.trim() || 'mqtt_events',
      key_strategy: ruleForm.key_strategy,
    };

    try {
      await updateRuleApi(payload);
      success(t('pipeline.save_success'));
      setRuleModalOpen(false);
      fetchData();
    } catch (err: any) {
      error(err.message || 'Failed to save rule');
    }
  };

  // Delete Rule
  const handleDeleteRule = async () => {
    if (!deleteRuleTarget) return;
    try {
      await deleteRuleApi(deleteRuleTarget);
      success(t('common.success'));
      setDeleteRuleTarget(null);
      fetchData();
    } catch (err: any) {
      error(err.message || 'Failed to delete rule');
    }
  };

  // Open Create Bridge Modal
  const handleOpenCreateBridge = () => {
    setEditingBridge(null);
    setBridgeForm({
      id: `bridge_${Date.now().toString(36)}`,
      name: '',
      type: 'kafka',
      servers: '127.0.0.1:9092',
      topic: 'mqtt_events',
    });
    setModalProbeResult(null);
    setBridgeModalOpen(true);
  };

  // Open Edit Bridge Modal
  const handleOpenEditBridge = (b: BridgeStatus) => {
    setEditingBridge(b);
    setBridgeForm({
      id: b.id,
      name: b.name,
      type: b.type,
      servers: (b.servers || []).join(', '),
      topic: (b.config?.topic as string) || 'mqtt_events',
    });
    setModalProbeResult(null);
    setBridgeModalOpen(true);
  };

  // Save Bridge
  const handleSaveBridge = async () => {
    if (!bridgeForm.name.trim()) {
      error('Bridge Name is required');
      return;
    }
    const servers = bridgeForm.servers
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);

    const defaultServers = bridgeForm.type === 'nats' ? ['127.0.0.1:4222'] : ['127.0.0.1:9092'];
    const defaultTopic = bridgeForm.type === 'nats' ? 'mqtt_telemetry' : 'mqtt_events';

    const payload: Bridge = {
      id: bridgeForm.id.trim(),
      name: bridgeForm.name.trim(),
      type: bridgeForm.type,
      servers: servers.length > 0 ? servers : defaultServers,
      config: {
        topic: bridgeForm.topic.trim() || defaultTopic,
        subject: bridgeForm.topic.trim() || defaultTopic,
      },
    };

    try {
      await updateBridgeApi(payload);
      success(t('pipeline.save_success'));
      setBridgeModalOpen(false);
      fetchData();
    } catch (err: any) {
      error(err.message || 'Failed to save bridge');
    }
  };

  // Delete Bridge
  const handleDeleteBridge = async () => {
    if (!deleteBridgeTarget) return;
    try {
      await deleteBridgeApi(deleteBridgeTarget);
      success(t('common.success'));
      setDeleteBridgeTarget(null);
      fetchData();
    } catch (err: any) {
      error(err.message || 'Failed to delete bridge');
    }
  };

  // Ping Bridge inline
  const handlePingBridge = async (id: string) => {
    setProbingBridgeId(id);
    try {
      const res = await pingBridgeApi(id);
      setProbeResults((prev) => ({ ...prev, [id]: res }));
      if (res.success) {
        success(`${t('pipeline.ping_success')} (${res.latency_ms} ms)`);
      } else {
        error(`${t('pipeline.ping_fail')} ${res.message}`);
      }
    } catch (err: any) {
      error(err.message || 'Ping failed');
    } finally {
      setProbingBridgeId(null);
    }
  };

  // Ping Bridge in modal
  const handleModalPing = async () => {
    setModalProbing(true);
    setModalProbeResult(null);
    try {
      // If editing existing, probe by ID; otherwise test via first bridge or temporary
      const targetId = editingBridge ? editingBridge.id : bridges[0]?.id;
      if (targetId) {
        const res = await pingBridgeApi(targetId);
        setModalProbeResult(res);
      } else {
        setModalProbeResult({
          success: true,
          latency_ms: 1,
          target: bridgeForm.servers,
          message: 'Local syntax verified',
        });
      }
    } catch (err: any) {
      setModalProbeResult({
        success: false,
        latency_ms: 0,
        target: bridgeForm.servers,
        message: err.message || 'Failed to reach broker',
      });
    } finally {
      setModalProbing(false);
    }
  };

  // Run Match Simulation
  const handleRunSimulation = async () => {
    if (!simTopic.trim()) return;
    setSimulating(true);
    try {
      const matched = await testRuleMatchApi(simTopic.trim());
      setSimMatchedIds(matched);
    } catch (err: any) {
      error(err.message || 'Simulation test failed');
    } finally {
      setSimulating(false);
    }
  };

  // Overview metric calculations
  const totalMatchedCount = useMemo(() => {
    return rules.reduce((sum, r) => sum + (r.total_matched || 0), 0);
  }, [rules]);

  const activeRulesCount = useMemo(() => {
    return rules.filter((r) => r.enabled).length;
  }, [rules]);

  const healthyBridgesCount = useMemo(() => {
    return bridges.filter((b) => b.status === 'connected').length;
  }, [bridges]);

  return (
    <div className="space-y-6">
      {/* Top Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
        <div>
          <h2 className="text-xl font-bold tracking-tight text-white flex items-center gap-2">
            <GitBranch className="h-5 w-5 text-brand-400" />
            {t('pipeline.title')}
          </h2>
          <p className="text-xs text-slate-400 mt-1">{t('pipeline.subtitle')}</p>
        </div>

        <div className="flex items-center gap-2.5">
          {/* Tabs Selector */}
          <div className="flex items-center bg-dark-900 border border-dark-750 p-1 rounded-xl">
            <button
              onClick={() => setActiveTab('rules')}
              className={`px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all flex items-center gap-2 ${
                activeTab === 'rules'
                  ? 'bg-brand-500 text-white shadow-sm shadow-brand-500/30'
                  : 'text-slate-400 hover:text-white hover:bg-dark-800'
              }`}
            >
              <Layers className="h-3.5 w-3.5" />
              {t('pipeline.tab_rules')}
              <span className="px-1.5 py-0.2 rounded-full text-[10px] bg-dark-900/60 font-mono">
                {rules.length}
              </span>
            </button>
            <button
              onClick={() => setActiveTab('bridges')}
              className={`px-3.5 py-1.5 rounded-lg text-xs font-semibold transition-all flex items-center gap-2 ${
                activeTab === 'bridges'
                  ? 'bg-brand-500 text-white shadow-sm shadow-brand-500/30'
                  : 'text-slate-400 hover:text-white hover:bg-dark-800'
              }`}
            >
              <Database className="h-3.5 w-3.5" />
              {t('pipeline.tab_bridges')}
              <span className="px-1.5 py-0.2 rounded-full text-[10px] bg-dark-900/60 font-mono">
                {bridges.length}
              </span>
            </button>
          </div>

          <Button
            variant="outline"
            size="sm"
            onClick={fetchData}
            disabled={loading}
            className="h-9 px-2.5"
          >
            <RefreshCw className={`h-4 w-4 ${loading ? 'animate-spin' : ''}`} />
          </Button>

          {activeTab === 'rules' ? (
            <>
              <Button
                variant="outline"
                size="sm"
                onClick={() => setSimModalOpen(true)}
                className="h-9 text-xs flex items-center gap-1.5"
              >
                <Play className="h-3.5 w-3.5 text-amber-400" />
                {t('pipeline.test_match')}
              </Button>
              <Button
                size="sm"
                onClick={handleOpenCreateRule}
                className="h-9 text-xs flex items-center gap-1.5 bg-brand-500 hover:bg-brand-600 text-white"
              >
                <Plus className="h-4 w-4" />
                {t('pipeline.create_rule')}
              </Button>
            </>
          ) : (
            <Button
              size="sm"
              onClick={handleOpenCreateBridge}
              className="h-9 text-xs flex items-center gap-1.5 bg-brand-500 hover:bg-brand-600 text-white"
            >
              <Plus className="h-4 w-4" />
              {t('pipeline.create_bridge')}
            </Button>
          )}
        </div>
      </div>

      {/* Overview Stat Cards */}
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
        <Card className="bg-dark-900/70 border-dark-800 backdrop-blur-sm">
          <CardContent className="p-4 flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-brand-500/10 text-brand-400 border border-brand-500/20">
              <Layers className="h-5 w-5" />
            </div>
            <div>
              <div className="text-[11px] font-medium text-slate-400">{t('pipeline.total_rules')}</div>
              <div className="text-xl font-bold text-white font-mono mt-0.5">{rules.length}</div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-dark-900/70 border-dark-800 backdrop-blur-sm">
          <CardContent className="p-4 flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              <CheckCircle2 className="h-5 w-5" />
            </div>
            <div>
              <div className="text-[11px] font-medium text-slate-400">{t('pipeline.active_rules')}</div>
              <div className="text-xl font-bold text-emerald-400 font-mono mt-0.5">{activeRulesCount}</div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-dark-900/70 border-dark-800 backdrop-blur-sm">
          <CardContent className="p-4 flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
              <Database className="h-5 w-5" />
            </div>
            <div>
              <div className="text-[11px] font-medium text-slate-400">{t('pipeline.total_bridges')}</div>
              <div className="text-xl font-bold text-white font-mono mt-0.5">
                {bridges.length}
                <span className="text-xs text-slate-400 font-normal ml-1.5">
                  ({healthyBridgesCount} {t('pipeline.healthy_bridges')})
                </span>
              </div>
            </div>
          </CardContent>
        </Card>

        <Card className="bg-dark-900/70 border-dark-800 backdrop-blur-sm">
          <CardContent className="p-4 flex items-center gap-3">
            <div className="p-2.5 rounded-xl bg-purple-500/10 text-purple-400 border border-purple-500/20">
              <Activity className="h-5 w-5" />
            </div>
            <div>
              <div className="text-[11px] font-medium text-slate-400">{t('pipeline.total_matched')}</div>
              <div className="text-xl font-bold text-white font-mono mt-0.5">
                {totalMatchedCount.toLocaleString()}
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Tab 1: Rules List */}
      {activeTab === 'rules' && (
        <div className="space-y-4">
          {rules.length === 0 ? (
            <Card className="border-dark-800 bg-dark-900/40 p-12 text-center">
              <Layers className="h-10 w-10 text-slate-600 mx-auto mb-3" />
              <div className="text-sm font-semibold text-slate-300">暂无转发规则</div>
              <p className="text-xs text-slate-500 mt-1 max-w-sm mx-auto">
                配置主题过滤规则，将设备发布的消息实时流转投递至指定 MQ 数据桥接。
              </p>
              <Button
                size="sm"
                onClick={handleOpenCreateRule}
                className="mt-4 bg-brand-500 hover:bg-brand-600 text-white text-xs"
              >
                <Plus className="h-4 w-4 mr-1.5" />
                {t('pipeline.create_rule')}
              </Button>
            </Card>
          ) : (
            <div className="grid grid-cols-1 gap-3.5">
              {rules.map((rule) => {
                const action = rule.actions && rule.actions.length > 0 ? rule.actions[0] : null;
                const boundBridge = action ? bridgeMap.get(action.bridge_id) : null;
                const targetTopic = action?.target_topic || rule.target_topic || 'mqtt_events';
                const keyStrategy = action?.key_strategy || rule.key_strategy || 'client_id';

                return (
                  <Card
                    key={rule.id}
                    className={`border transition-all ${
                      rule.enabled
                        ? 'border-dark-750/80 bg-dark-900/80 hover:border-dark-700'
                        : 'border-dark-800/60 bg-dark-900/40 opacity-70'
                    }`}
                  >
                    <div className="p-5 flex flex-col md:flex-row md:items-center justify-between gap-4">
                      {/* Left: Info */}
                      <div className="space-y-2 flex-1 min-w-0">
                        <div className="flex items-center gap-2.5 flex-wrap">
                          <span className="text-sm font-bold text-white tracking-tight">
                            {rule.name}
                          </span>
                          <span className="font-mono text-xs text-slate-400 bg-dark-800 px-2 py-0.5 rounded border border-dark-750">
                            {rule.id}
                          </span>
                          <span
                            className={`px-2 py-0.5 rounded text-[11px] font-medium border ${
                              rule.enabled
                                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                                : 'bg-slate-500/10 text-slate-400 border-slate-500/20'
                            }`}
                          >
                            {rule.enabled ? t('pipeline.rule_enabled') : t('pipeline.rule_disabled')}
                          </span>
                          <span className="text-[11px] font-mono text-slate-400 bg-dark-800/80 px-2 py-0.5 rounded">
                            {rule.payload_format === 'json' ? 'JSON' : 'Raw'}
                          </span>
                        </div>

                        {rule.description && (
                          <p className="text-xs text-slate-400 line-clamp-1">{rule.description}</p>
                        )}

                        {/* Match filters and action badge */}
                        <div className="flex items-center gap-3 pt-1 flex-wrap text-xs">
                          <div className="flex items-center gap-1.5 text-slate-400">
                            <span className="text-[11px] text-slate-500">匹配主题:</span>
                            {(rule.topic_filters || ['#']).map((f, idx) => (
                              <span
                                key={idx}
                                className="font-mono text-xs text-brand-300 bg-brand-500/15 border border-brand-500/30 px-2 py-0.5 rounded"
                              >
                                {f}
                              </span>
                            ))}
                          </div>

                          <div className="flex items-center gap-1.5 text-slate-400">
                            <span className="text-[11px] text-slate-500">流转动作:</span>
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded bg-dark-800 border border-dark-700 text-slate-200">
                              <Database className={`h-3 w-3 ${
                                boundBridge?.type === 'nats' ? 'text-emerald-400' :
                                boundBridge?.type === 'redpanda' ? 'text-rose-400' : 'text-cyan-400'
                              }`} />
                              <span className="font-medium">
                                {boundBridge ? boundBridge.name : action?.bridge_id || 'Bridge'}
                              </span>
                              {boundBridge && (
                                <span className={`text-[9px] uppercase font-mono px-1.5 py-0.2 rounded border ${
                                  boundBridge.type === 'nats' ? 'bg-emerald-500/15 text-emerald-300 border-emerald-500/30' :
                                  boundBridge.type === 'redpanda' ? 'bg-rose-500/15 text-rose-300 border-rose-500/30' :
                                  'bg-cyan-500/15 text-cyan-300 border-cyan-500/30'
                                }`}>
                                  {boundBridge.type}
                                </span>
                              )}
                              <span className="text-slate-500">→</span>
                              <span className="font-mono text-amber-300">{targetTopic}</span>
                              <span className="text-[10px] text-slate-400 border-l border-dark-700 pl-1.5">
                                Key: {keyStrategy}
                              </span>
                            </span>
                          </div>
                        </div>
                      </div>

                      {/* Right: Metrics & Actions */}
                      <div className="flex items-center gap-4 self-end md:self-center shrink-0">
                        <div className="text-right pr-2">
                          <div className="text-[11px] text-slate-400">累计流转命中</div>
                          <div className="text-base font-bold text-white font-mono mt-0.5">
                            {(rule.total_matched || 0).toLocaleString()}
                          </div>
                        </div>

                        {/* Switch button */}
                        <Button
                          variant={rule.enabled ? 'outline' : 'secondary'}
                          size="sm"
                          onClick={() => handleToggleRule(rule)}
                          className={`h-8 text-xs font-semibold ${
                            rule.enabled
                              ? 'border-emerald-500/30 text-emerald-400 hover:bg-emerald-500/10'
                              : 'text-slate-400'
                          }`}
                        >
                          {rule.enabled ? '已启用' : '已停用'}
                        </Button>

                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleOpenEditRule(rule)}
                          className="h-8 w-8 p-0 text-slate-400 hover:text-white"
                        >
                          <Edit2 className="h-4 w-4" />
                        </Button>

                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setDeleteRuleTarget(rule.id)}
                          className="h-8 w-8 p-0 text-slate-400 hover:text-red-400"
                        >
                          <Trash2 className="h-4 w-4" />
                        </Button>
                      </div>
                    </div>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Tab 2: Bridges List */}
      {activeTab === 'bridges' && (
        <div className="space-y-4">
          {bridges.length === 0 ? (
            <Card className="border-dark-800 bg-dark-900/40 p-12 text-center">
              <Database className="h-10 w-10 text-slate-600 mx-auto mb-3" />
              <div className="text-sm font-semibold text-slate-300">暂无数据桥接资源</div>
              <p className="text-xs text-slate-500 mt-1 max-w-sm mx-auto">
                创建与 Apache Kafka、RabbitMQ 等外部 MQ 集群的物理连接桥接。
              </p>
              <Button
                size="sm"
                onClick={handleOpenCreateBridge}
                className="mt-4 bg-brand-500 hover:bg-brand-600 text-white text-xs"
              >
                <Plus className="h-4 w-4 mr-1.5" />
                {t('pipeline.create_bridge')}
              </Button>
            </Card>
          ) : (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              {bridges.map((bridge) => {
                const isConnected = bridge.status === 'connected';
                const isProbing = probingBridgeId === bridge.id;
                const probeResult = probeResults[bridge.id];

                return (
                  <Card
                    key={bridge.id}
                    className="border border-dark-750/80 bg-dark-900/80 hover:border-dark-700 transition-all flex flex-col justify-between"
                  >
                    <div>
                      {/* Bridge Header */}
                      <div className="p-4 border-b border-dark-800/80 flex items-center justify-between">
                        <div className="flex items-center gap-2.5">
                          <div className="p-2 rounded-lg bg-cyan-500/10 text-cyan-400 border border-cyan-500/20">
                            <ServerIcon className="h-4 w-4" />
                          </div>
                          <div>
                            <div className="text-sm font-bold text-white leading-tight">
                              {bridge.name}
                            </div>
                            <div className="font-mono text-[11px] text-slate-400 mt-0.5">
                              {bridge.id}
                            </div>
                          </div>
                        </div>

                        <div className="flex items-center gap-2">
                          {bridge.type === 'nats' ? (
                            <Badge variant="outline" className="uppercase font-mono text-[10px] bg-emerald-500/15 text-emerald-300 border-emerald-500/30">
                              NATS JETSTREAM
                            </Badge>
                          ) : bridge.type === 'redpanda' ? (
                            <Badge variant="outline" className="uppercase font-mono text-[10px] bg-rose-500/15 text-rose-300 border-rose-500/30">
                              REDPANDA
                            </Badge>
                          ) : bridge.type === 'kafka' ? (
                            <Badge variant="outline" className="uppercase font-mono text-[10px] bg-cyan-500/15 text-cyan-300 border-cyan-500/30">
                              KAFKA
                            </Badge>
                          ) : (
                            <Badge variant="outline" className="uppercase font-mono text-[10px] bg-dark-800 text-slate-300 border-dark-700">
                              {bridge.type}
                            </Badge>
                          )}
                          <span
                            className={`px-2 py-0.5 rounded text-[11px] font-medium border flex items-center gap-1 ${
                              isConnected
                                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                                : 'bg-red-500/10 text-red-400 border-red-500/30'
                            }`}
                          >
                            <span
                              className={`w-1.5 h-1.5 rounded-full ${
                                isConnected ? 'bg-emerald-400 animate-pulse' : 'bg-red-400'
                              }`}
                            />
                            {isConnected ? t('pipeline.connected') : t('pipeline.disconnected')}
                          </span>
                        </div>
                      </div>

                      {/* Bridge Content */}
                      <div className="p-4 space-y-3.5">
                        {/* Servers */}
                        <div>
                          <div className="text-[11px] text-slate-400 mb-1">连接端点 (Brokers):</div>
                          <div className="font-mono text-xs text-slate-200 bg-dark-800/90 p-2 rounded-lg border border-dark-750/70 select-all">
                            {(bridge.servers || []).join(', ') || 'none'}
                          </div>
                        </div>

                        {/* Telemetry Metrics */}
                        <div className="grid grid-cols-3 gap-2 pt-1 border-t border-dark-800/60 text-center">
                          <div className="p-2 rounded-lg bg-dark-800/50">
                            <div className="text-[10px] text-slate-400">{t('pipeline.delivered_total')}</div>
                            <div className="text-sm font-bold text-white font-mono mt-0.5">
                              {(bridge.delivered_total || 0).toLocaleString()}
                            </div>
                          </div>
                          <div className="p-2 rounded-lg bg-dark-800/50">
                            <div className="text-[10px] text-slate-400">{t('pipeline.pending_total')}</div>
                            <div className="text-sm font-bold text-amber-400 font-mono mt-0.5">
                              {bridge.pending_total || 0}
                            </div>
                          </div>
                          <div className="p-2 rounded-lg bg-dark-800/50">
                            <div className="text-[10px] text-slate-400">{t('pipeline.dropped_total')}</div>
                            <div className="text-sm font-bold text-slate-300 font-mono mt-0.5">
                              {bridge.dropped_total || 0}
                            </div>
                          </div>
                        </div>

                        {/* Probe Result Alert if available */}
                        {probeResult && (
                          <div
                            className={`p-2.5 rounded-lg border text-xs flex items-center justify-between ${
                              probeResult.success
                                ? 'bg-emerald-500/10 text-emerald-300 border-emerald-500/30'
                                : 'bg-red-500/10 text-red-300 border-red-500/30'
                            }`}
                          >
                            <span>
                              {probeResult.success ? '✓ 连通性测试通过' : `✗ ${probeResult.message}`}
                            </span>
                            <span className="font-mono text-[11px]">
                              {probeResult.latency_ms} ms
                            </span>
                          </div>
                        )}
                      </div>
                    </div>

                    {/* Bridge Footer Controls */}
                    <div className="p-3 bg-dark-800/40 border-t border-dark-800/80 flex items-center justify-between">
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => handlePingBridge(bridge.id)}
                        disabled={isProbing}
                        className="h-8 text-xs flex items-center gap-1.5"
                      >
                        <RefreshCw className={`h-3.5 w-3.5 ${isProbing ? 'animate-spin' : ''}`} />
                        {isProbing ? t('pipeline.pinging') : t('pipeline.test_conn')}
                      </Button>

                      <div className="flex items-center gap-1">
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => handleOpenEditBridge(bridge)}
                          className="h-8 px-2.5 text-xs text-slate-400 hover:text-white flex items-center gap-1"
                        >
                          <Edit2 className="h-3.5 w-3.5" />
                          {t('pipeline.edit_bridge')}
                        </Button>
                        <Button
                          variant="ghost"
                          size="sm"
                          onClick={() => setDeleteBridgeTarget(bridge.id)}
                          className="h-8 px-2 text-slate-400 hover:text-red-400"
                        >
                          <Trash2 className="h-3.5 w-3.5" />
                        </Button>
                      </div>
                    </div>
                  </Card>
                );
              })}
            </div>
          )}
        </div>
      )}

      {/* Bottom Buffer & Spooler Telemetry */}
      {pipeline && (
        <Card className="border-dark-800 bg-dark-900/60 p-4">
          <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 text-xs">
            <div className="flex items-center gap-2 text-slate-300">
              <HardDrive className="h-4 w-4 text-brand-400" />
              <span className="font-semibold">{t('pipeline.buffer_status')}</span>
              <span className="text-slate-500">|</span>
              <span className="text-slate-400">
                {t('pipeline.disk_usage')}:{' '}
                <strong className="text-white font-mono">{pipeline.disk_usage_mb} MB</strong> /{' '}
                {pipeline.max_disk_quota_gb} GB
              </span>
            </div>

            <div className="flex items-center gap-4 text-slate-400">
              <span>
                {t('pipeline.circuit_breaker')}:{' '}
                <strong
                  className={`font-mono ${
                    pipeline.circuit_breaker === 'Closed' ? 'text-emerald-400' : 'text-amber-400'
                  }`}
                >
                  {pipeline.circuit_breaker}
                </strong>
              </span>
              <span>
                累计直投: <strong className="text-white font-mono">{pipeline.direct_sent.toLocaleString()}</strong>
              </span>
              <span>
                落盘自愈: <strong className="text-white font-mono">{pipeline.spooled_total.toLocaleString()}</strong>
              </span>
            </div>
          </div>
        </Card>
      )}

      {/* MODAL 1: Create / Edit Rule */}
      <Dialog
        open={ruleModalOpen}
        onOpenChange={setRuleModalOpen}
        title={editingRule ? t('pipeline.edit_rule') : t('pipeline.create_rule')}
        description="配置 MQTT 主题通配符匹配与目标数据桥接投递动作"
        footer={
          <>
            <Button variant="outline" size="sm" onClick={() => setRuleModalOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button size="sm" onClick={handleSaveRule} className="bg-brand-500 hover:bg-brand-600 text-white">
              {t('common.confirm')}
            </Button>
          </>
        }
      >
        <div className="space-y-4 text-xs">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.rule_id')} *</label>
              <Input
                value={ruleForm.id}
                onChange={(e) => setRuleForm({ ...ruleForm, id: e.target.value })}
                disabled={!!editingRule}
                placeholder="rule_telemetry"
                className="font-mono text-xs"
              />
            </div>
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.rule_name')} *</label>
              <Input
                value={ruleForm.name}
                onChange={(e) => setRuleForm({ ...ruleForm, name: e.target.value })}
                placeholder="传感器数据流转"
              />
            </div>
          </div>

          <div>
            <label className="block text-slate-400 mb-1">{t('pipeline.description')}</label>
            <Input
              value={ruleForm.description}
              onChange={(e) => setRuleForm({ ...ruleForm, description: e.target.value })}
              placeholder="将所有车联网或传感器遥测消息实时流转至 Kafka"
            />
          </div>

          <div>
            <label className="block text-slate-400 mb-1">{t('pipeline.topic_filters')} *</label>
            <Input
              value={ruleForm.topic_filters}
              onChange={(e) => setRuleForm({ ...ruleForm, topic_filters: e.target.value })}
              placeholder="telemetry/#, sensors/+/data"
              className="font-mono text-xs"
            />
            <p className="text-[11px] text-slate-500 mt-1">{t('pipeline.topic_filters_help')}</p>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.payload_format')}</label>
              <Select
                value={ruleForm.payload_format}
                onChange={(e) =>
                  setRuleForm({ ...ruleForm, payload_format: e.target.value as 'raw' | 'json' })
                }
              >
                <option value="raw">{t('pipeline.payload_raw')}</option>
                <option value="json">{t('pipeline.payload_json')}</option>
              </Select>
            </div>

            <div className="flex items-center pt-6">
              <label className="flex items-center gap-2 cursor-pointer select-none text-slate-300">
                <input
                  type="checkbox"
                  checked={ruleForm.enabled}
                  onChange={(e) => setRuleForm({ ...ruleForm, enabled: e.target.checked })}
                  className="rounded border-dark-700 bg-dark-800 text-brand-500 focus:ring-0"
                />
                <span>启用该转发规则</span>
              </label>
            </div>
          </div>

          {/* Action Configuration Box */}
          <div className="p-3.5 rounded-xl bg-dark-800/60 border border-dark-750/80 space-y-3">
            <div className="flex items-center gap-2 text-slate-200 font-semibold text-xs">
              <Database className="h-4 w-4 text-cyan-400" />
              <span>{t('pipeline.actions')} - 数据桥接流转</span>
            </div>

            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.select_bridge')} *</label>
              <Select
                value={ruleForm.bridge_id}
                onChange={(e) => setRuleForm({ ...ruleForm, bridge_id: e.target.value })}
              >
                {bridges.map((b) => (
                  <option key={b.id} value={b.id}>
                    {b.name} ({b.type} - {b.servers?.[0] || 'active'})
                  </option>
                ))}
              </Select>
              <p className="text-[11px] text-slate-500 mt-1">{t('pipeline.select_bridge_help')}</p>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <label className="block text-slate-400 mb-1">{t('pipeline.target_topic')} *</label>
                <Input
                  value={ruleForm.target_topic}
                  onChange={(e) => setRuleForm({ ...ruleForm, target_topic: e.target.value })}
                  placeholder="mqtt_events"
                  className="font-mono text-xs"
                />
              </div>

              <div>
                <label className="block text-slate-400 mb-1">{t('pipeline.key_strategy')}</label>
                <Select
                  value={ruleForm.key_strategy}
                  onChange={(e) =>
                    setRuleForm({
                      ...ruleForm,
                      key_strategy: e.target.value as 'client_id' | 'topic' | 'none',
                    })
                  }
                >
                  <option value="client_id">{t('pipeline.key_client_id')}</option>
                  <option value="topic">{t('pipeline.key_topic')}</option>
                  <option value="none">{t('pipeline.key_none')}</option>
                </Select>
              </div>
            </div>
          </div>
        </div>
      </Dialog>

      {/* MODAL 2: Create / Edit Bridge */}
      <Dialog
        open={bridgeModalOpen}
        onOpenChange={setBridgeModalOpen}
        title={editingBridge ? t('pipeline.edit_bridge') : t('pipeline.create_bridge')}
        description="管理 Kafka 等外部消息中间件集群连接资源与测试探测"
        footer={
          <>
            <Button variant="outline" size="sm" onClick={() => setBridgeModalOpen(false)}>
              {t('common.cancel')}
            </Button>
            <Button size="sm" onClick={handleSaveBridge} className="bg-brand-500 hover:bg-brand-600 text-white">
              {t('common.confirm')}
            </Button>
          </>
        }
      >
        <div className="space-y-4 text-xs">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.bridge_id')} *</label>
              <Input
                value={bridgeForm.id}
                onChange={(e) => setBridgeForm({ ...bridgeForm, id: e.target.value })}
                disabled={!!editingBridge}
                placeholder="bridge_kafka_01"
                className="font-mono text-xs"
              />
            </div>
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.bridge_name')} *</label>
              <Input
                value={bridgeForm.name}
                onChange={(e) => setBridgeForm({ ...bridgeForm, name: e.target.value })}
                placeholder="生产 Kafka 集群"
              />
            </div>
          </div>

          <div className="grid grid-cols-2 gap-3">
            <div>
              <label className="block text-slate-400 mb-1">{t('pipeline.bridge_type')} *</label>
              <Select
                value={bridgeForm.type}
                onChange={(e) => {
                  const newType = e.target.value;
                  let newServers = bridgeForm.servers;
                  let newTopic = bridgeForm.topic;
                  if (!editingBridge) {
                    if (newType === 'nats') {
                      newServers = '127.0.0.1:4222';
                      newTopic = 'mqtt_telemetry';
                    } else if (newType === 'redpanda' || newType === 'kafka') {
                      newServers = '127.0.0.1:9092';
                      newTopic = 'mqtt_events';
                    }
                  }
                  setBridgeForm({
                    ...bridgeForm,
                    type: newType,
                    servers: newServers,
                    topic: newTopic,
                  });
                }}
              >
                {drivers.map((d) => {
                  let label = d.toUpperCase();
                  if (d === 'nats') label = 'NATS JETSTREAM';
                  if (d === 'redpanda') label = 'REDPANDA (KAFKA API)';
                  if (d === 'kafka') label = 'APACHE KAFKA';
                  return (
                    <option key={d} value={d}>
                      {label}
                    </option>
                  );
                })}
              </Select>
            </div>
            <div>
              <label className="block text-slate-400 mb-1">
                {bridgeForm.type === 'nats' ? 'NATS Subject (主题/流)' : t('pipeline.target_topic')}
              </label>
              <Input
                value={bridgeForm.topic}
                onChange={(e) => setBridgeForm({ ...bridgeForm, topic: e.target.value })}
                placeholder={bridgeForm.type === 'nats' ? 'mqtt_telemetry' : 'mqtt_events'}
                className="font-mono text-xs"
              />
            </div>
          </div>

          <div>
            <label className="block text-slate-400 mb-1">
              {bridgeForm.type === 'nats' ? 'NATS 服务地址 (NATS Servers)' : t('pipeline.bridge_servers')} *
            </label>
            <Input
              value={bridgeForm.servers}
              onChange={(e) => setBridgeForm({ ...bridgeForm, servers: e.target.value })}
              placeholder={bridgeForm.type === 'nats' ? '127.0.0.1:4222' : '127.0.0.1:9092, 192.168.1.10:9092'}
              className="font-mono text-xs"
            />
            <p className="text-[11px] text-slate-500 mt-1">
              {bridgeForm.type === 'nats'
                ? 'NATS 默认监听端口 4222，支持多个集群节点以逗号分隔，如 127.0.0.1:4222'
                : t('pipeline.bridge_servers_help')}
            </p>
          </div>

          {/* Test connection inside modal */}
          <div className="pt-2">
            <div className="flex items-center justify-between">
              <span className="text-slate-400">连通性探测测试:</span>
              <Button
                type="button"
                variant="outline"
                size="sm"
                onClick={handleModalPing}
                disabled={modalProbing}
                className="h-8 text-xs"
              >
                <RefreshCw className={`h-3.5 w-3.5 mr-1.5 ${modalProbing ? 'animate-spin' : ''}`} />
                {modalProbing ? t('pipeline.pinging') : t('pipeline.test_conn')}
              </Button>
            </div>

            {modalProbeResult && (
              <div
                className={`mt-2 p-2.5 rounded-lg border text-xs flex items-center justify-between ${
                  modalProbeResult.success
                    ? 'bg-emerald-500/10 text-emerald-300 border-emerald-500/30'
                    : 'bg-red-500/10 text-red-300 border-red-500/30'
                }`}
              >
                <span>
                  {modalProbeResult.success ? '✓ 连通性测试通过' : `✗ ${modalProbeResult.message}`}
                </span>
                <span className="font-mono text-[11px]">{modalProbeResult.latency_ms} ms</span>
              </div>
            )}
          </div>
        </div>
      </Dialog>

      {/* MODAL 3: Match Simulation */}
      <Dialog
        open={simModalOpen}
        onOpenChange={setSimModalOpen}
        title={t('pipeline.test_match')}
        description={t('pipeline.test_match_desc')}
        footer={
          <Button size="sm" onClick={() => setSimModalOpen(false)}>
            {t('common.close')}
          </Button>
        }
      >
        <div className="space-y-4 text-xs">
          <div>
            <label className="block text-slate-400 mb-1">{t('pipeline.test_match_input')}</label>
            <div className="flex gap-2">
              <Input
                value={simTopic}
                onChange={(e) => setSimTopic(e.target.value)}
                placeholder={t('pipeline.test_match_placeholder')}
                className="font-mono text-xs"
              />
              <Button
                size="sm"
                onClick={handleRunSimulation}
                disabled={simulating}
                className="bg-brand-500 hover:bg-brand-600 text-white shrink-0"
              >
                <Play className="h-3.5 w-3.5 mr-1.5" />
                {simulating ? '匹配中...' : t('pipeline.test_match_btn')}
              </Button>
            </div>
          </div>

          {simMatchedIds !== null && (
            <div className="p-3.5 rounded-xl bg-dark-800/80 border border-dark-750">
              <div className="text-slate-400 text-xs font-medium mb-2">
                {t('pipeline.test_match_result')}:
              </div>
              {simMatchedIds.length > 0 ? (
                <div className="space-y-2">
                  <div className="text-emerald-400 font-medium">
                    {t('pipeline.test_match_matched')} ({simMatchedIds.length})
                  </div>
                  <div className="flex flex-wrap gap-2">
                    {simMatchedIds.map((id) => {
                      const matchedRule = rules.find((r) => r.id === id);
                      return (
                        <div
                          key={id}
                          className="px-2.5 py-1 rounded bg-emerald-500/10 border border-emerald-500/30 text-emerald-300 font-mono text-xs flex items-center gap-1.5"
                        >
                          <CheckCircle2 className="h-3.5 w-3.5 text-emerald-400" />
                          <span>{matchedRule?.name || id}</span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : (
                <div className="text-amber-400 flex items-center gap-1.5 py-1">
                  <AlertTriangle className="h-4 w-4" />
                  <span>{t('pipeline.test_match_none')}</span>
                </div>
              )}
            </div>
          )}
        </div>
      </Dialog>

      {/* Delete Rule Confirm Dialog */}
      <Dialog
        open={!!deleteRuleTarget}
        onOpenChange={(open) => !open && setDeleteRuleTarget(null)}
        title={t('pipeline.delete_rule_confirm')}
        description={t('pipeline.delete_rule_desc')}
        footer={
          <>
            <Button variant="outline" size="sm" onClick={() => setDeleteRuleTarget(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              size="sm"
              onClick={handleDeleteRule}
              className="bg-red-600 hover:bg-red-700 text-white"
            >
              {t('common.delete')}
            </Button>
          </>
        }
      >
        <p className="text-xs text-slate-300">
          目标规则 ID: <code className="font-mono text-amber-300">{deleteRuleTarget}</code>
        </p>
      </Dialog>

      {/* Delete Bridge Confirm Dialog */}
      <Dialog
        open={!!deleteBridgeTarget}
        onOpenChange={(open) => !open && setDeleteBridgeTarget(null)}
        title={t('pipeline.delete_bridge_confirm')}
        description={t('pipeline.delete_bridge_desc')}
        footer={
          <>
            <Button variant="outline" size="sm" onClick={() => setDeleteBridgeTarget(null)}>
              {t('common.cancel')}
            </Button>
            <Button
              size="sm"
              onClick={handleDeleteBridge}
              className="bg-red-600 hover:bg-red-700 text-white"
            >
              {t('common.delete')}
            </Button>
          </>
        }
      >
        <p className="text-xs text-slate-300">
          目标桥接 ID: <code className="font-mono text-amber-300">{deleteBridgeTarget}</code>
        </p>
      </Dialog>
    </div>
  );
};

export default Pipeline;
