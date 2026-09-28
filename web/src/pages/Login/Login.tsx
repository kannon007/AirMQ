import React, { useState } from 'react';
import { Sliders, Lock, User, AlertCircle, ArrowRight } from 'lucide-react';
import { useI18n } from '../../i18n/I18nContext';
import { loginApi } from '../../api/client';
import { Button } from '../../components/ui/button';
import { Input } from '../../components/ui/input';

interface LoginProps {
  onLoginSuccess: (token: string, username: string) => void;
}

export const Login: React.FC<LoginProps> = ({ onLoginSuccess }) => {
  const { t } = useI18n();
  const [username, setUsername] = useState('admin');
  const [password, setPassword] = useState('public');
  const [loading, setLoading] = useState(false);
  const [errorMsg, setErrorMsg] = useState('');

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMsg('');
    setLoading(true);

    try {
      const res = await loginApi(username, password);
      onLoginSuccess(res.token, res.username);
    } catch (err: any) {
      setErrorMsg(err.message || t('auth.login_failed'));
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen w-full flex items-center justify-center p-4 bg-gradient-to-br from-dark-950 via-dark-900 to-dark-950 relative overflow-hidden">
      {/* Background ambient lighting */}
      <div className="absolute -top-40 -left-40 w-96 h-96 rounded-full bg-brand-500/10 blur-[120px] pointer-events-none" />
      <div className="absolute -bottom-40 -right-40 w-96 h-96 rounded-full bg-cyan-500/10 blur-[120px] pointer-events-none" />

      <div className="w-full max-w-md relative z-10">
        <div className="rounded-2xl border border-dark-750 bg-dark-900/90 backdrop-blur-xl p-8 shadow-2xl shadow-black/60">
          {/* Brand header */}
          <div className="text-center mb-8">
            <div className="inline-flex items-center justify-center w-14 h-14 rounded-2xl bg-brand-500/15 border border-brand-500/30 text-brand-400 mb-4 shadow-lg shadow-brand-500/20">
              <Sliders className="h-7 w-7" />
            </div>
            <h1 className="text-xl font-bold text-white tracking-tight">
              {t('auth.login_title')}
            </h1>
            <p className="text-xs text-slate-400 mt-1.5">
              {t('auth.login_subtitle')}
            </p>
          </div>

          {/* Form */}
          <form onSubmit={handleSubmit} className="space-y-4">
            {errorMsg && (
              <div className="p-3 rounded-lg bg-rose-500/10 border border-rose-500/30 text-rose-300 text-xs flex items-center gap-2">
                <AlertCircle className="h-4 w-4 shrink-0" />
                <span>{errorMsg}</span>
              </div>
            )}

            <div className="space-y-1.5">
              <label className="text-xs font-medium text-slate-300 flex items-center gap-1.5">
                <User className="h-3.5 w-3.5 text-slate-400" />
                <span>{t('auth.username')}</span>
              </label>
              <Input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
                className="h-10 text-sm"
              />
            </div>

            <div className="space-y-1.5">
              <label className="text-xs font-medium text-slate-300 flex items-center gap-1.5">
                <Lock className="h-3.5 w-3.5 text-slate-400" />
                <span>{t('auth.password')}</span>
              </label>
              <Input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
                className="h-10 text-sm"
              />
            </div>

            <Button
              type="submit"
              className="w-full h-10 mt-2 text-sm font-semibold gap-2"
              disabled={loading}
            >
              <span>{loading ? t('auth.logging_in') : t('auth.login_btn')}</span>
              <ArrowRight className="h-4 w-4" />
            </Button>
          </form>

          {/* Hint */}
          <div className="mt-6 pt-4 border-t border-dark-800 text-center">
            <p className="text-xs text-slate-500 font-mono">
              {t('auth.default_hint')}
            </p>
          </div>
        </div>
      </div>
    </div>
  );
};
