import React, { useState, useEffect } from 'react';
import { I18nProvider } from './i18n/I18nContext';
import { ToastProvider } from './components/ui/toast';
import { Sidebar, PageId } from './components/layout/Sidebar';
import { Header } from './components/layout/Header';
import { Login } from './pages/Login/Login';
import { Overview } from './pages/Overview/Overview';
import { Clients } from './pages/Clients/Clients';
import { Subscriptions } from './pages/Subscriptions/Subscriptions';
import { Retained } from './pages/Retained/Retained';
import { Listeners } from './pages/Listeners/Listeners';
import { Cluster } from './pages/Cluster/Cluster';
import { Pipeline } from './pages/Pipeline/Pipeline';
import { Tools } from './pages/Tools/Tools';
import { logoutApi, getMeApi, getOverviewApi } from './api/client';

export const AppContent: React.FC = () => {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem('mqtt_token'));
  const [username, setUsername] = useState<string>(() => localStorage.getItem('mqtt_user') || 'admin');
  const [nodeName, setNodeName] = useState<string>('standalone-node');
  const [activePage, setActivePage] = useState<PageId>('overview');
  const [refreshKey, setRefreshKey] = useState<number>(0);
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false);

  useEffect(() => {
    const handleUnauthorized = () => {
      setToken(null);
    };
    window.addEventListener('auth:unauthorized', handleUnauthorized);
    return () => window.removeEventListener('auth:unauthorized', handleUnauthorized);
  }, []);

  useEffect(() => {
    if (token) {
      getMeApi()
        .then((profile) => {
          if (profile?.username) {
            setUsername(profile.username);
            localStorage.setItem('mqtt_user', profile.username);
          }
        })
        .catch(() => {});

      getOverviewApi()
        .then((data) => {
          if (data?.node_name) {
            setNodeName(data.node_name);
          }
        })
        .catch(() => {});
    }
  }, [token, refreshKey]);

  const handleLoginSuccess = (newToken: string, user: string) => {
    localStorage.setItem('mqtt_token', newToken);
    localStorage.setItem('mqtt_user', user);
    setToken(newToken);
    setUsername(user);
  };

  const handleLogout = async () => {
    await logoutApi();
    setToken(null);
  };

  const handleManualRefresh = () => {
    setIsRefreshing(true);
    setRefreshKey((k) => k + 1);
    setTimeout(() => setIsRefreshing(false), 600);
  };

  if (!token) {
    return <Login onLoginSuccess={handleLoginSuccess} />;
  }

  const renderActivePage = () => {
    switch (activePage) {
      case 'overview':
        return <Overview key={refreshKey} />;
      case 'clients':
        return <Clients key={refreshKey} />;
      case 'subscriptions':
        return <Subscriptions key={refreshKey} />;
      case 'retained':
        return <Retained key={refreshKey} />;
      case 'listeners':
        return <Listeners key={refreshKey} />;
      case 'cluster':
        return <Cluster key={refreshKey} />;
      case 'pipeline':
        return <Pipeline key={refreshKey} />;
      case 'tools':
        return <Tools key={refreshKey} />;
      default:
        return <Overview key={refreshKey} />;
    }
  };

  return (
    <div className="flex h-screen w-screen overflow-hidden bg-dark-950 font-sans text-slate-100">
      {/* Left Sidebar */}
      <Sidebar activePage={activePage} onPageChange={setActivePage} />

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Top Header */}
        <Header
          nodeName={nodeName}
          username={username}
          onLogout={handleLogout}
          onRefresh={handleManualRefresh}
          isRefreshing={isRefreshing}
        />

        {/* Scrollable Page Body */}
        <main className="flex-1 overflow-y-auto p-6 bg-gradient-to-b from-dark-900/40 to-dark-950">
          <div className="max-w-7xl mx-auto">{renderActivePage()}</div>
        </main>
      </div>
    </div>
  );
};

export const App: React.FC = () => {
  return (
    <I18nProvider>
      <ToastProvider>
        <AppContent />
      </ToastProvider>
    </I18nProvider>
  );
};

export default App;
