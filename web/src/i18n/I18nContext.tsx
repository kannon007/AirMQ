import React, { createContext, useContext, useState, useEffect } from 'react';
import { zhCN } from './locales/zh-CN';
import { enUS } from './locales/en-US';

export type Language = 'zh-CN' | 'en-US';

interface I18nContextType {
  lang: Language;
  setLang: (lang: Language) => void;
  t: (key: string, vars?: Record<string, string | number>) => string;
}

const dictionaries = {
  'zh-CN': zhCN,
  'en-US': enUS,
};

const I18nContext = createContext<I18nContextType | null>(null);

export const I18nProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [lang, setLangState] = useState<Language>(() => {
    const saved = localStorage.getItem('mqtt_lang');
    if (saved === 'en-US' || saved === 'zh-CN') {
      return saved;
    }
    return navigator.language.startsWith('zh') ? 'zh-CN' : 'en-US';
  });

  const setLang = (newLang: Language) => {
    setLangState(newLang);
    localStorage.setItem('mqtt_lang', newLang);
  };

  useEffect(() => {
    document.documentElement.lang = lang;
  }, [lang]);

  const t = (key: string, vars?: Record<string, string | number>): string => {
    const parts = key.split('.');
    let current: any = dictionaries[lang];
    for (const part of parts) {
      if (current && typeof current === 'object' && part in current) {
        current = current[part];
      } else {
        // Fallback to zh-CN if missing in en-US
        let fallback: any = dictionaries['zh-CN'];
        for (const p of parts) {
          if (fallback && typeof fallback === 'object' && p in fallback) {
            fallback = fallback[p];
          } else {
            return key;
          }
        }
        return fallback;
      }
    }

    if (typeof current === 'string' && vars) {
      return Object.entries(vars).reduce((acc, [k, v]) => {
        return acc.replace(new RegExp(`{${k}}`, 'g'), String(v));
      }, current);
    }

    return typeof current === 'string' ? current : key;
  };

  return (
    <I18nContext.Provider value={{ lang, setLang, t }}>
      {children}
    </I18nContext.Provider>
  );
};

export function useI18n(): I18nContextType {
  const ctx = useContext(I18nContext);
  if (!ctx) {
    throw new Error('useI18n must be used within I18nProvider');
  }
  return ctx;
}
