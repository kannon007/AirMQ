import React, { useState, useEffect, useRef, useMemo } from 'react';
import mqtt, { MqttClient } from 'mqtt';
import {
  Radio,
  Send,
  Wifi,
  WifiOff,
  RefreshCw,
  Plus,
  Trash2,
  Copy,
  Check,
  Play,
  Square,
  Sparkles,
  ArrowDownLeft,
  ArrowUpRight,
  Filter,
  Layers,
  Activity,
  Sliders,
  AlertCircle,
  Eye,
} from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { Card, CardHeader, CardTitle, CardContent } from '../../components/ui/card';
import { Badge } from '../../components/ui/badge';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';
import { Textarea } from '../../components/ui/textarea';
import { Select } from '../../components/ui/select';
import { useToast } from '../../components/ui/toast';

interface MessageLog {
  id: string;
  timestamp: string;
  direction: 'in' | 'out';
  topic: string;
  qos: number;
  retain: boolean;
  payload: string;
}

interface SubscriptionItem {
  topic: string;
  qos: 0 | 1 | 2;
}

const PRESET_TEMPLATES = [
  {
    name: '🌡️ 工业传感器 (IoT Sensor)',
    topic: 'factory/sensor/temperature',
    payload: {
      device_id: 'sensor-alpha-01',
      temperature: 25.4,
      humidity: 58.2,
      voltage: 3.3,
      status: 'normal',
      timestamp: Date.now(),
    },
  },
  {
    name: '🚗 车联网车机遥测 (Connected Vehicle)',
    topic: 'telemetry/vehicle/speed',
    payload: {
      vin: 'VIN8892019481',
      speed_kmh: 72.5,
      battery_pct: 84,
      gear: 'D',
      gps: { lat: 31.2304, lng: 121.4737 },
      timestamp: Date.now(),
    },
  },
  {
    name: '💡 智能家居开关 (Smart Switch)',
    topic: 'home/living_room/light',
    payload: {
      switch: 'ON',
      brightness: 80,
      color_temp: 4000,
      mode: 'reading',
    },
  },
  {
    name: '⚡ 设备告警事件 (Device Alert)',
    topic: 'alerts/device/critical',
    payload: {
      alert_id: 'ALT_0091',
      severity: 'WARNING',
      error_code: 4012,
      message: 'Motor temperature exceeding safety threshold',
      timestamp: Date.now(),
    },
  },
];

export const Tools: React.FC = () => {
  const { t } = useI18n();
  const { success, error, info } = useToast();

  // Connection State
  const defaultWsHost = typeof window !== 'undefined' ? window.location.hostname || '127.0.0.1' : '127.0.0.1';
  const defaultWsProtocol = typeof window !== 'undefined' && window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const [wsUrl, setWsUrl] = useState(`${defaultWsProtocol}//${defaultWsHost}:8083/mqtt`);
  const [clientId, setClientId] = useState(`sim_${Math.random().toString(16).slice(2, 8)}`);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [keepalive, setKeepalive] = useState(60);
  const [cleanSession, setCleanSession] = useState(true);
  const [showAdvanced, setShowAdvanced] = useState(false);

  // Client & Status
  const [status, setStatus] = useState<'disconnected' | 'connecting' | 'connected' | 'error'>('disconnected');
  const [statusError, setStatusError] = useState<string>('');
  const clientRef = useRef<MqttClient | null>(null);

  // Subscriptions
  const [subTopic, setSubTopic] = useState('factory/#');
  const [subQos, setSubQos] = useState<0 | 1 | 2>(0);
  const [subscriptions, setSubscriptions] = useState<SubscriptionItem[]>([]);

  // Publishing
  const [pubTopic, setPubTopic] = useState('factory/sensor/temperature');
  const [pubQos, setPubQos] = useState<0 | 1 | 2>(0);
  const [pubRetain, setPubRetain] = useState(false);
  const [payload, setPayload] = useState(
    JSON.stringify(PRESET_TEMPLATES[0].payload, null, 2)
  );
  const [publishing, setPublishing] = useState(false);

  // Auto Simulation
  const [autoSimulating, setAutoSimulating] = useState(false);
  const [autoInterval, setAutoInterval] = useState(1000);
  const autoTimerRef = useRef<number | null>(null);
  const seqRef = useRef(1);

  // Message Logs Stream
  const [messages, setMessages] = useState<MessageLog[]>([]);
  const [filterText, setFilterText] = useState('');
  const [copiedId, setCopiedId] = useState<string | null>(null);

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (autoTimerRef.current) {
        clearInterval(autoTimerRef.current);
      }
      if (clientRef.current) {
        clientRef.current.end(true);
      }
    };
  }, []);

  // Regenerate Client ID
  const handleRegenerateClientId = () => {
    setClientId(`sim_${Math.random().toString(16).slice(2, 8)}`);
  };

  // Connect to MQTT Broker
  const handleConnect = () => {
    if (status === 'connected' || status === 'connecting') return;

    setStatus('connecting');
    setStatusError('');

    try {
      const client = mqtt.connect(wsUrl, {
        clientId,
        username: username.trim() || undefined,
        password: password || undefined,
        keepalive: Number(keepalive) || 60,
        clean: cleanSession,
        reconnectPeriod: 0,
        connectTimeout: 5000,
      });

      client.on('connect', () => {
        setStatus('connected');
        success(t('tools.connected'));
        // Re-subscribe if any
        subscriptions.forEach((sub) => {
          client.subscribe(sub.topic, { qos: sub.qos });
        });
      });

      client.on('message', (topic, payloadBuf, packet) => {
        const text = payloadBuf.toString();
        const newMsg: MessageLog = {
          id: Math.random().toString(36).slice(2),
          timestamp: new Date().toLocaleTimeString(),
          direction: 'in',
          topic,
          qos: packet.qos,
          retain: packet.retain,
          payload: text,
        };
        setMessages((prev) => [newMsg, ...prev.slice(0, 199)]);
      });

      client.on('error', (err) => {
        setStatus('error');
        setStatusError(err.message || 'Connection failed');
        error(`${t('tools.error')}: ${err.message}`);
      });

      client.on('close', () => {
        setStatus('disconnected');
      });

      clientRef.current = client;
    } catch (err: any) {
      setStatus('error');
      setStatusError(err.message || 'Initialization failed');
      error(err.message);
    }
  };

  // Disconnect from MQTT Broker
  const handleDisconnect = () => {
    if (autoSimulating) {
      stopAutoSimulation();
    }
    if (clientRef.current) {
      clientRef.current.end(true);
      clientRef.current = null;
    }
    setStatus('disconnected');
    setSubscriptions([]);
    info(t('tools.disconnected'));
  };

  // Subscribe to topic
  const handleSubscribe = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    const cleanTopic = subTopic.trim();
    if (!cleanTopic) return;

    if (!clientRef.current || status !== 'connected') {
      error('请先连接至 MQTT Broker');
      return;
    }

    if (subscriptions.some((s) => s.topic === cleanTopic)) {
      info(`已存在订阅: ${cleanTopic}`);
      return;
    }

    clientRef.current.subscribe(cleanTopic, { qos: subQos }, (err) => {
      if (err) {
        error(`订阅失败: ${err.message}`);
      } else {
        setSubscriptions((prev) => [...prev, { topic: cleanTopic, qos: subQos }]);
        success(`成功订阅 ${cleanTopic} (QoS ${subQos})`);
        setSubTopic('');
      }
    });
  };

  // Unsubscribe
  const handleUnsubscribe = (topicToUnsub: string) => {
    if (clientRef.current && status === 'connected') {
      clientRef.current.unsubscribe(topicToUnsub);
    }
    setSubscriptions((prev) => prev.filter((s) => s.topic !== topicToUnsub));
    info(`已取消订阅 ${topicToUnsub}`);
  };

  // Single Publish
  const handlePublish = (e?: React.FormEvent) => {
    if (e) e.preventDefault();
    const cleanTopic = pubTopic.trim();
    if (!cleanTopic) {
      error('主题不能为空');
      return;
    }
    if (cleanTopic.includes('+') || cleanTopic.includes('#')) {
      error('发布主题不能包含通配符 (+ 或 #)');
      return;
    }
    if (!clientRef.current || status !== 'connected') {
      error('请先连接至 MQTT Broker');
      return;
    }

    setPublishing(true);
    clientRef.current.publish(
      cleanTopic,
      payload,
      { qos: pubQos, retain: pubRetain },
      (err) => {
        setPublishing(false);
        if (err) {
          error(`发送失败: ${err.message}`);
        } else {
          const newMsg: MessageLog = {
            id: Math.random().toString(36).slice(2),
            timestamp: new Date().toLocaleTimeString(),
            direction: 'out',
            topic: cleanTopic,
            qos: pubQos,
            retain: pubRetain,
            payload,
          };
          setMessages((prev) => [newMsg, ...prev.slice(0, 199)]);
          success('发送成功');
        }
      }
    );
  };

  // Start periodic auto-simulation
  const startAutoSimulation = () => {
    if (!clientRef.current || status !== 'connected') {
      error('请先连接至 MQTT Broker');
      return;
    }

    setAutoSimulating(true);
    autoTimerRef.current = window.setInterval(() => {
      if (!clientRef.current || status !== 'connected') {
        stopAutoSimulation();
        return;
      }

      const seq = seqRef.current++;
      let currentPayload = payload;

      try {
        const obj = JSON.parse(payload);
        if (typeof obj.temperature === 'number') {
          obj.temperature = +(24 + Math.random() * 4).toFixed(1);
        }
        if (typeof obj.speed_kmh === 'number') {
          obj.speed_kmh = +(60 + Math.random() * 30).toFixed(1);
        }
        if (typeof obj.humidity === 'number') {
          obj.humidity = +(50 + Math.random() * 15).toFixed(1);
        }
        obj.timestamp = Date.now();
        obj.seq = seq;
        currentPayload = JSON.stringify(obj, null, 2);
      } catch {
        // keep text
      }

      clientRef.current.publish(pubTopic.trim(), currentPayload, {
        qos: pubQos,
        retain: pubRetain,
      });

      const newMsg: MessageLog = {
        id: Math.random().toString(36).slice(2),
        timestamp: new Date().toLocaleTimeString(),
        direction: 'out',
        topic: pubTopic.trim(),
        qos: pubQos,
        retain: pubRetain,
        payload: currentPayload,
      };
      setMessages((prev) => [newMsg, ...prev.slice(0, 199)]);
    }, autoInterval);

    success(`周期模拟推流已启动 (${autoInterval}ms)`);
  };

  // Stop periodic auto-simulation
  const stopAutoSimulation = () => {
    if (autoTimerRef.current) {
      clearInterval(autoTimerRef.current);
      autoTimerRef.current = null;
    }
    setAutoSimulating(false);
    info('周期模拟已停止');
  };

  // Apply Preset Template
  const handleApplyPreset = (tpl: (typeof PRESET_TEMPLATES)[0]) => {
    setPubTopic(tpl.topic);
    setPayload(JSON.stringify(tpl.payload, null, 2));
    success(`已加载模版: ${tpl.name}`);
  };

  // Format JSON
  const handleFormatJson = () => {
    try {
      const parsed = JSON.parse(payload);
      setPayload(JSON.stringify(parsed, null, 2));
      success('JSON 格式化成功');
    } catch {
      error('当前内容不是有效的 JSON');
    }
  };

  // Copy payload
  const handleCopy = (text: string, id: string) => {
    navigator.clipboard.writeText(text);
    setCopiedId(id);
    setTimeout(() => setCopiedId(null), 1500);
  };

  // Filter messages
  const filteredMessages = useMemo(() => {
    if (!filterText.trim()) return messages;
    const q = filterText.toLowerCase();
    return messages.filter(
      (m) => m.topic.toLowerCase().includes(q) || m.payload.toLowerCase().includes(q)
    );
  }, [messages, filterText]);

  // Counters
  const inCount = useMemo(() => messages.filter((m) => m.direction === 'in').length, [messages]);
  const outCount = useMemo(() => messages.filter((m) => m.direction === 'out').length, [messages]);

  return (
    <div className="space-y-5">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 p-4 rounded-xl border border-dark-750 bg-dark-900/80">
        <div>
          <h2 className="text-base font-bold text-white flex items-center gap-2">
            <Radio className="h-5 w-5 text-brand-400" />
            {t('tools.title')}
          </h2>
          <p className="text-xs text-slate-400 mt-0.5">{t('tools.subtitle')}</p>
        </div>

        {/* Global Connection Badge */}
        <div className="flex items-center gap-3">
          <div
            className={`px-3 py-1.5 rounded-lg border text-xs font-semibold flex items-center gap-2 ${
              status === 'connected'
                ? 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30'
                : status === 'connecting'
                ? 'bg-amber-500/10 text-amber-400 border-amber-500/30'
                : status === 'error'
                ? 'bg-red-500/10 text-red-400 border-red-500/30'
                : 'bg-dark-800 text-slate-400 border-dark-700'
            }`}
          >
            <span
              className={`w-2 h-2 rounded-full ${
                status === 'connected'
                  ? 'bg-emerald-400 animate-pulse'
                  : status === 'connecting'
                  ? 'bg-amber-400 animate-spin'
                  : status === 'error'
                  ? 'bg-red-400'
                  : 'bg-slate-500'
              }`}
            />
            {status === 'connected'
              ? t('tools.connected')
              : status === 'connecting'
              ? t('tools.connecting')
              : status === 'error'
              ? t('tools.error')
              : t('tools.disconnected')}
          </div>
        </div>
      </div>

      {/* Main Grid: Left Controls (5 cols) & Right Log Stream (7 cols) */}
      <div className="grid grid-cols-1 lg:grid-cols-12 gap-5 items-start">
        {/* LEFT COLUMN: Connection, Subscriptions, Publishing */}
        <div className="lg:col-span-5 space-y-5">
          {/* Card 1: Connection Config */}
          <Card className="border-dark-750 bg-dark-900/80">
            <CardHeader className="p-4 pb-3 border-b border-dark-800 flex flex-row items-center justify-between">
              <CardTitle className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                <Wifi className="h-4 w-4 text-brand-400" />
                {t('tools.conn_config')}
              </CardTitle>
              <button
                type="button"
                onClick={() => setShowAdvanced(!showAdvanced)}
                className="text-[11px] text-brand-400 hover:text-brand-300 flex items-center gap-1"
              >
                <Sliders className="h-3 w-3" />
                <span>{showAdvanced ? '简略设置' : '高级选项'}</span>
              </button>
            </CardHeader>
            <CardContent className="p-4 space-y-3 text-xs">
              <div>
                <label className="block text-slate-400 mb-1">{t('tools.ws_url')} *</label>
                <Input
                  value={wsUrl}
                  onChange={(e) => setWsUrl(e.target.value)}
                  disabled={status === 'connected' || status === 'connecting'}
                  className="font-mono text-xs"
                />
              </div>

              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="text-slate-400">{t('tools.client_id')} *</label>
                  <button
                    type="button"
                    onClick={handleRegenerateClientId}
                    disabled={status === 'connected' || status === 'connecting'}
                    className="text-[11px] text-slate-400 hover:text-white"
                  >
                    随机生成
                  </button>
                </div>
                <Input
                  value={clientId}
                  onChange={(e) => setClientId(e.target.value)}
                  disabled={status === 'connected' || status === 'connecting'}
                  className="font-mono text-xs"
                />
              </div>

              {/* Advanced params */}
              {showAdvanced && (
                <div className="pt-2 border-t border-dark-800 space-y-3">
                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="block text-slate-400 mb-1">{t('tools.username')}</label>
                      <Input
                        value={username}
                        onChange={(e) => setUsername(e.target.value)}
                        disabled={status === 'connected'}
                        placeholder="admin"
                        className="text-xs"
                      />
                    </div>
                    <div>
                      <label className="block text-slate-400 mb-1">{t('tools.password')}</label>
                      <Input
                        type="password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        disabled={status === 'connected'}
                        placeholder="••••••"
                        className="text-xs"
                      />
                    </div>
                  </div>

                  <div className="grid grid-cols-2 gap-2">
                    <div>
                      <label className="block text-slate-400 mb-1">{t('tools.keepalive')}</label>
                      <Input
                        type="number"
                        value={keepalive}
                        onChange={(e) => setKeepalive(Number(e.target.value))}
                        disabled={status === 'connected'}
                        className="text-xs font-mono"
                      />
                    </div>
                    <div className="flex items-center pt-5">
                      <label className="flex items-center gap-2 cursor-pointer text-slate-300">
                        <input
                          type="checkbox"
                          checked={cleanSession}
                          onChange={(e) => setCleanSession(e.target.checked)}
                          disabled={status === 'connected'}
                          className="rounded border-dark-700 bg-dark-800 text-brand-500"
                        />
                        <span>{t('tools.clean_session')}</span>
                      </label>
                    </div>
                  </div>
                </div>
              )}

              {/* Error note if failed */}
              {statusError && (
                <div className="p-2 rounded bg-red-500/10 border border-red-500/30 text-red-300 text-[11px] flex items-center gap-1.5">
                  <AlertCircle className="h-3.5 w-3.5 shrink-0" />
                  <span className="truncate">{statusError}</span>
                </div>
              )}

              {/* Connect / Disconnect Action Button */}
              <div className="pt-1">
                {status === 'connected' ? (
                  <Button
                    variant="outline"
                    onClick={handleDisconnect}
                    className="w-full h-8 text-xs border-red-500/30 text-red-400 hover:bg-red-500/10 font-semibold flex items-center justify-center gap-1.5"
                  >
                    <WifiOff className="h-3.5 w-3.5" />
                    {t('tools.disconnect')}
                  </Button>
                ) : (
                  <Button
                    onClick={handleConnect}
                    disabled={status === 'connecting'}
                    className="w-full h-8 text-xs bg-brand-500 hover:bg-brand-600 text-white font-semibold flex items-center justify-center gap-1.5 shadow-sm shadow-brand-500/20"
                  >
                    <Wifi className={`h-3.5 w-3.5 ${status === 'connecting' ? 'animate-spin' : ''}`} />
                    {status === 'connecting' ? t('tools.connecting') : t('tools.connect')}
                  </Button>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Card 2: Subscriptions */}
          <Card className="border-dark-750 bg-dark-900/80">
            <CardHeader className="p-4 pb-3 border-b border-dark-800">
              <CardTitle className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                <Layers className="h-4 w-4 text-cyan-400" />
                {t('tools.subs_title')}
              </CardTitle>
            </CardHeader>
            <CardContent className="p-4 space-y-3 text-xs">
              <form onSubmit={handleSubscribe} className="flex gap-2">
                <Input
                  value={subTopic}
                  onChange={(e) => setSubTopic(e.target.value)}
                  placeholder={t('tools.sub_topic_placeholder')}
                  className="font-mono text-xs flex-1"
                />
                <select
                  value={subQos}
                  onChange={(e) => setSubQos(Number(e.target.value) as 0 | 1 | 2)}
                  className="rounded-md border border-dark-700 bg-dark-900 px-2 py-1 text-xs text-slate-300 font-mono"
                >
                  <option value={0}>QoS 0</option>
                  <option value={1}>QoS 1</option>
                  <option value={2}>QoS 2</option>
                </select>
                <Button
                  type="submit"
                  size="sm"
                  disabled={status !== 'connected' || !subTopic.trim()}
                  className="h-9 px-3 bg-cyan-600 hover:bg-cyan-700 text-white text-xs shrink-0"
                >
                  <Plus className="h-3.5 w-3.5 mr-1" />
                  {t('tools.subscribe_btn')}
                </Button>
              </form>

              {/* Subscriptions List */}
              <div className="pt-1">
                <div className="text-[11px] text-slate-500 mb-1.5">{t('tools.active_subs')} ({subscriptions.length}):</div>
                {subscriptions.length === 0 ? (
                  <div className="text-[11px] text-slate-500 italic p-2 rounded bg-dark-800/40 border border-dark-800">
                    {t('tools.no_subs')}
                  </div>
                ) : (
                  <div className="flex flex-wrap gap-1.5">
                    {subscriptions.map((sub) => (
                      <span
                        key={sub.topic}
                        className="inline-flex items-center gap-1.5 px-2 py-1 rounded bg-cyan-500/10 border border-cyan-500/30 text-cyan-300 font-mono text-xs"
                      >
                        <span>{sub.topic}</span>
                        <span className="text-[10px] text-cyan-400 bg-cyan-950/60 px-1 rounded">
                          Q{sub.qos}
                        </span>
                        <button
                          type="button"
                          onClick={() => handleUnsubscribe(sub.topic)}
                          className="text-slate-400 hover:text-red-400 ml-0.5"
                          title={t('tools.unsubscribe')}
                        >
                          ✕
                        </button>
                      </span>
                    ))}
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          {/* Card 3: Publish & Simulation */}
          <Card className="border-dark-750 bg-dark-900/80">
            <CardHeader className="p-4 pb-3 border-b border-dark-800 flex flex-row items-center justify-between">
              <CardTitle className="text-xs font-bold text-slate-200 uppercase tracking-wider flex items-center gap-2">
                <Send className="h-4 w-4 text-emerald-400" />
                {t('tools.pub_title')}
              </CardTitle>

              {/* Quick Presets Dropdown */}
              <div className="flex items-center gap-1.5">
                <span className="text-[11px] text-slate-400">模版:</span>
                <select
                  onChange={(e) => {
                    const idx = Number(e.target.value);
                    if (idx >= 0 && PRESET_TEMPLATES[idx]) {
                      handleApplyPreset(PRESET_TEMPLATES[idx]);
                    }
                  }}
                  defaultValue="-1"
                  className="rounded border border-dark-700 bg-dark-800 px-2 py-0.5 text-[11px] text-slate-200"
                >
                  <option value="-1" disabled>选择常用场景...</option>
                  {PRESET_TEMPLATES.map((tpl, i) => (
                    <option key={i} value={i}>
                      {tpl.name}
                    </option>
                  ))}
                </select>
              </div>
            </CardHeader>
            <CardContent className="p-4 space-y-3.5 text-xs">
              <div>
                <label className="block text-slate-400 mb-1">{t('tools.topic_label')} *</label>
                <Input
                  value={pubTopic}
                  onChange={(e) => setPubTopic(e.target.value)}
                  placeholder={t('tools.topic_placeholder')}
                  className="font-mono text-xs"
                />
              </div>

              {/* QoS & Retain */}
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-slate-400 mb-1">{t('tools.qos_label')}</label>
                  <div className="grid grid-cols-3 gap-1">
                    {[0, 1, 2].map((q) => (
                      <button
                        key={q}
                        type="button"
                        onClick={() => setPubQos(q as 0 | 1 | 2)}
                        className={`py-1 text-xs font-mono font-semibold rounded border transition-all ${
                          pubQos === q
                            ? 'bg-brand-500/20 border-brand-500 text-brand-300'
                            : 'bg-dark-800 border-dark-700 text-slate-400 hover:text-white'
                        }`}
                      >
                        QoS {q}
                      </button>
                    ))}
                  </div>
                </div>

                <div>
                  <label className="block text-slate-400 mb-1">{t('tools.retain_label')}</label>
                  <button
                    type="button"
                    onClick={() => setPubRetain(!pubRetain)}
                    className={`w-full py-1 text-xs font-semibold rounded border transition-all ${
                      pubRetain
                        ? 'bg-amber-500/20 border-amber-500 text-amber-300'
                        : 'bg-dark-800 border-dark-700 text-slate-400 hover:text-white'
                    }`}
                  >
                    {pubRetain ? 'Retain = True' : 'Retain = False'}
                  </button>
                </div>
              </div>

              {/* Payload Editor */}
              <div>
                <div className="flex items-center justify-between mb-1">
                  <label className="text-slate-400">{t('tools.payload_label')}</label>
                  <button
                    type="button"
                    onClick={handleFormatJson}
                    className="text-[11px] text-brand-400 hover:text-brand-300 flex items-center gap-1"
                  >
                    <Sparkles className="h-3 w-3" />
                    <span>{t('tools.format_json')}</span>
                  </button>
                </div>
                <Textarea
                  rows={5}
                  value={payload}
                  onChange={(e) => setPayload(e.target.value)}
                  placeholder={t('tools.payload_placeholder')}
                  className="font-mono text-xs text-emerald-300 bg-dark-950/60 leading-relaxed"
                />
              </div>

              {/* Periodic Auto Simulation Bar */}
              <div className="p-2.5 rounded-lg bg-dark-800/60 border border-dark-750 flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <Activity className="h-3.5 w-3.5 text-amber-400" />
                  <span className="text-[11px] text-slate-300 font-medium">{t('tools.auto_pub')}:</span>
                  <select
                    value={autoInterval}
                    onChange={(e) => setAutoInterval(Number(e.target.value))}
                    disabled={autoSimulating}
                    className="rounded border border-dark-700 bg-dark-900 px-2 py-0.5 text-[11px] text-slate-300 font-mono"
                  >
                    <option value={500}>500 ms</option>
                    <option value={1000}>1 秒 / 次</option>
                    <option value={2000}>2 秒 / 次</option>
                    <option value={5000}>5 秒 / 次</option>
                  </select>
                </div>

                {autoSimulating ? (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={stopAutoSimulation}
                    className="h-7 text-xs border-amber-500/40 text-amber-400 hover:bg-amber-500/10 gap-1"
                  >
                    <Square className="h-3 w-3 fill-current" />
                    <span>{t('tools.stop_auto')}</span>
                  </Button>
                ) : (
                  <Button
                    size="sm"
                    variant="outline"
                    onClick={startAutoSimulation}
                    disabled={status !== 'connected'}
                    className="h-7 text-xs text-emerald-400 hover:bg-emerald-500/10 gap-1 border-emerald-500/30"
                  >
                    <Play className="h-3 w-3 fill-current" />
                    <span>{t('tools.start_auto')}</span>
                  </Button>
                )}
              </div>

              {/* Publish Action Button */}
              <Button
                onClick={handlePublish}
                disabled={publishing || status !== 'connected' || autoSimulating}
                className="w-full h-8 text-xs bg-emerald-600 hover:bg-emerald-700 text-white font-semibold flex items-center justify-center gap-1.5 shadow-sm shadow-emerald-600/20"
              >
                <Send className={`h-3.5 w-3.5 ${publishing ? 'animate-spin' : ''}`} />
                <span>{publishing ? t('tools.publishing') : t('tools.publish_btn')}</span>
              </Button>
            </CardContent>
          </Card>
        </div>

        {/* RIGHT COLUMN: Live Messages Stream (7 cols) */}
        <div className="lg:col-span-7">
          <Card className="border-dark-750 bg-dark-900/80 flex flex-col h-[760px]">
            {/* Header & Controls */}
            <CardHeader className="p-4 pb-3 border-b border-dark-800 shrink-0">
              <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                  <Activity className="h-4 w-4 text-brand-400" />
                  <CardTitle className="text-xs font-bold text-slate-200 uppercase tracking-wider">
                    {t('tools.stream_title')}
                  </CardTitle>
                  <span className="text-xs text-slate-500 font-mono">
                    ({filteredMessages.length})
                  </span>
                </div>

                {/* Stream Stats & Clear */}
                <div className="flex items-center gap-2 text-xs">
                  <span className="text-[11px] text-emerald-400 bg-emerald-500/10 border border-emerald-500/20 px-2 py-0.5 rounded">
                    ⬇ {t('tools.total_received')}: <strong>{inCount}</strong>
                  </span>
                  <span className="text-[11px] text-cyan-400 bg-cyan-500/10 border border-cyan-500/20 px-2 py-0.5 rounded">
                    ⬆ {t('tools.total_sent')}: <strong>{outCount}</strong>
                  </span>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setMessages([])}
                    className="h-7 text-xs text-slate-400 hover:text-white px-2 gap-1"
                  >
                    <Trash2 className="h-3 w-3" />
                    <span>{t('tools.clear_logs')}</span>
                  </Button>
                </div>
              </div>

              {/* Filter Bar */}
              <div className="pt-2">
                <div className="relative">
                  <Filter className="absolute left-2.5 top-2.5 h-3.5 w-3.5 text-slate-500" />
                  <Input
                    value={filterText}
                    onChange={(e) => setFilterText(e.target.value)}
                    placeholder={t('tools.filter_placeholder')}
                    className="pl-8 h-8 text-xs bg-dark-950/60 font-mono"
                  />
                </div>
              </div>
            </CardHeader>

            {/* Scrollable Message List */}
            <CardContent className="p-4 flex-1 overflow-y-auto space-y-2.5 font-mono text-xs">
              {filteredMessages.length === 0 ? (
                <div className="h-full flex flex-col items-center justify-center text-center p-8 text-slate-500">
                  <Radio className="h-10 w-10 text-slate-600 mb-2" />
                  <p className="max-w-xs text-xs leading-relaxed">{t('tools.no_messages')}</p>
                </div>
              ) : (
                filteredMessages.map((msg) => (
                  <div
                    key={msg.id}
                    className={`p-3 rounded-xl border transition-all ${
                      msg.direction === 'in'
                        ? 'bg-emerald-950/20 border-emerald-500/30'
                        : 'bg-cyan-950/20 border-cyan-500/30'
                    }`}
                  >
                    {/* Message Header */}
                    <div className="flex items-center justify-between gap-2 pb-1.5 border-b border-dark-800/80 text-[11px]">
                      <div className="flex items-center gap-2 flex-wrap">
                        <span
                          className={`px-1.5 py-0.2 rounded font-bold uppercase text-[10px] flex items-center gap-0.5 ${
                            msg.direction === 'in'
                              ? 'bg-emerald-500/20 text-emerald-400'
                              : 'bg-cyan-500/20 text-cyan-400'
                          }`}
                        >
                          {msg.direction === 'in' ? (
                            <>
                              <ArrowDownLeft className="h-3 w-3" />
                              {t('tools.direction_in')}
                            </>
                          ) : (
                            <>
                              <ArrowUpRight className="h-3 w-3" />
                              {t('tools.direction_out')}
                            </>
                          )}
                        </span>

                        <span className="text-white font-bold tracking-tight">
                          {msg.topic}
                        </span>

                        <span className="text-[10px] text-slate-400 bg-dark-800 px-1 rounded">
                          QoS {msg.qos}
                        </span>

                        {msg.retain && (
                          <span className="text-[10px] text-amber-400 bg-amber-500/10 px-1 rounded">
                            Retain
                          </span>
                        )}
                      </div>

                      <div className="flex items-center gap-2 text-slate-500 shrink-0">
                        <span>{msg.timestamp}</span>
                        <button
                          type="button"
                          onClick={() => handleCopy(msg.payload, msg.id)}
                          className="text-slate-400 hover:text-white p-0.5 rounded hover:bg-dark-800"
                          title="复制 Payload"
                        >
                          {copiedId === msg.id ? (
                            <Check className="h-3.5 w-3.5 text-emerald-400" />
                          ) : (
                            <Copy className="h-3.5 w-3.5" />
                          )}
                        </button>
                      </div>
                    </div>

                    {/* Payload Body */}
                    <div className="pt-2">
                      <pre className="text-xs text-slate-200 whitespace-pre-wrap break-all bg-dark-950/70 p-2.5 rounded-lg border border-dark-800/80 max-h-48 overflow-y-auto leading-relaxed">
                        {msg.payload}
                      </pre>
                    </div>
                  </div>
                ))
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
};

export default Tools;
