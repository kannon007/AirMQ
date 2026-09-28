import axios, { AxiosError } from 'axios';
import {
  ApiResponse,
  OverviewStats,
  ClientSummary,
  ClientDetail,
  SubscriptionSummary,
  RetainedSummary,
  ListenerSummary,
  ClusterSummary,
  PipelineSummary,
  PaginatedResult,
  PublishRequest,
  LoginResponse,
  UserProfile,
  Rule,
  RuleStatus,
  RulePingResult,
  Bridge,
  BridgeStatus,
  MatchTestResult,
} from '../types/api';

const api = axios.create({
  baseURL: '/api/v1',
  timeout: 10000,
});

// Request Interceptor: Attach Token
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('mqtt_token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

// Response Interceptor: Unwrap data & catch 401
api.interceptors.response.use(
  (response) => {
    return response;
  },
  (error: AxiosError<ApiResponse>) => {
    if (error.response?.status === 401) {
      localStorage.removeItem('mqtt_token');
      localStorage.removeItem('mqtt_user');
      window.dispatchEvent(new Event('auth:unauthorized'));
    }
    const message = error.response?.data?.message || error.message || 'Network request failed';
    return Promise.reject(new Error(message));
  }
);

// Auth
export async function loginApi(username: string, password: string):Promise<LoginResponse> {
  const res = await api.post<ApiResponse<LoginResponse>>('/auth/login', { username, password });
  return res.data.data;
}

export async function logoutApi(): Promise<void> {
  try {
    await api.post('/auth/logout');
  } finally {
    localStorage.removeItem('mqtt_token');
    localStorage.removeItem('mqtt_user');
  }
}

export async function getMeApi(): Promise<UserProfile> {
  const res = await api.get<ApiResponse<UserProfile>>('/auth/me');
  return res.data.data;
}

// Overview
export async function getOverviewApi(): Promise<OverviewStats> {
  const res = await api.get<ApiResponse<OverviewStats>>('/overview');
  return res.data.data;
}

// Clients
export async function getClientsApi(page = 1, limit = 20, query = ''): Promise<PaginatedResult<ClientSummary>> {
  const res = await api.get<ApiResponse<PaginatedResult<ClientSummary>>>('/clients', {
    params: { page, limit, query },
  });
  return res.data.data;
}

export async function getClientDetailApi(clientId: string): Promise<ClientDetail> {
  const res = await api.get<ApiResponse<ClientDetail>>(`/clients/${encodeURIComponent(clientId)}`);
  return res.data.data;
}

export async function kickClientApi(clientId: string): Promise<void> {
  await api.delete(`/clients/${encodeURIComponent(clientId)}`);
}

// Subscriptions
export async function getSubscriptionsApi(page = 1, limit = 20, query = ''): Promise<PaginatedResult<SubscriptionSummary>> {
  const res = await api.get<ApiResponse<PaginatedResult<SubscriptionSummary>>>('/subscriptions', {
    params: { page, limit, query },
  });
  return res.data.data;
}

export async function unsubscribeClientApi(clientId: string, topic: string): Promise<void> {
  await api.delete('/subscriptions', {
    params: { client_id: clientId, topic },
  });
}

// Retained
export async function getRetainedApi(): Promise<RetainedSummary[]> {
  const res = await api.get<ApiResponse<{ items: RetainedSummary[]; count: number }>>('/retained');
  return res.data.data.items || [];
}

export async function deleteRetainedApi(topic: string): Promise<void> {
  await api.delete('/retained', {
    params: { topic },
  });
}

// Direct Publish
export async function publishMessageApi(req: PublishRequest): Promise<void> {
  await api.post('/publish', req);
}

// System / Listeners
export async function getListenersApi(): Promise<ListenerSummary[]> {
  const res = await api.get<ApiResponse<{ items: ListenerSummary[]; count: number }>>('/listeners');
  return res.data.data.items || [];
}

// System / Cluster
export async function getClusterNodesApi(): Promise<ClusterSummary> {
  const res = await api.get<ApiResponse<ClusterSummary>>('/cluster/nodes');
  return res.data.data;
}

// System / Pipeline
export async function getPipelineApi(): Promise<PipelineSummary> {
  const res = await api.get<ApiResponse<PipelineSummary>>('/pipeline');
  return res.data.data;
}

// Rules & Integration
export async function getRulesApi(): Promise<RuleStatus[]> {
  const res = await api.get<ApiResponse<{ items: RuleStatus[]; count: number }>>('/rules');
  return res.data.data.items || [];
}

export async function getRuleApi(id: string): Promise<RuleStatus> {
  const res = await api.get<ApiResponse<RuleStatus>>(`/rules/${encodeURIComponent(id)}`);
  return res.data.data;
}

export async function updateRuleApi(rule: Rule): Promise<void> {
  await api.put('/rules', rule);
}

export async function deleteRuleApi(id: string): Promise<void> {
  await api.delete(`/rules/${encodeURIComponent(id)}`);
}

export async function pingRuleApi(id: string): Promise<RulePingResult> {
  const res = await api.post<ApiResponse<RulePingResult>>(`/rules/${encodeURIComponent(id)}/test`);
  return res.data.data;
}

export async function testRuleMatchApi(topic: string): Promise<string[]> {
  const res = await api.post<ApiResponse<MatchTestResult>>('/rules/match-test', { topic });
  return res.data.data.matched_rule_ids || [];
}

// Data Bridges
export async function getBridgesApi(): Promise<BridgeStatus[]> {
  const res = await api.get<ApiResponse<{ items: BridgeStatus[]; count: number }>>('/bridges');
  return res.data.data.items || [];
}

export async function getBridgeApi(id: string): Promise<BridgeStatus> {
  const res = await api.get<ApiResponse<BridgeStatus>>(`/bridges/${encodeURIComponent(id)}`);
  return res.data.data;
}

export async function updateBridgeApi(bridge: Bridge): Promise<void> {
  await api.put('/bridges', bridge);
}

export async function deleteBridgeApi(id: string): Promise<void> {
  await api.delete(`/bridges/${encodeURIComponent(id)}`);
}

export async function pingBridgeApi(id: string): Promise<RulePingResult> {
  const res = await api.post<ApiResponse<RulePingResult>>(`/bridges/${encodeURIComponent(id)}/test`);
  return res.data.data;
}

export async function getDriversApi(): Promise<string[]> {
  const res = await api.get<ApiResponse<{ drivers: string[] }>>('/rules/drivers');
  return res.data.data.drivers || [];
}

export default api;
