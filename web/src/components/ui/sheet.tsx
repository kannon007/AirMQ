import * as React from 'react';
import { X } from 'lucide-react';
import { cn } from './button';

interface SheetProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  children: React.ReactNode;
  width?: string;
}

export function Sheet({
  open,
  onOpenChange,
  title,
  description,
  children,
  width = 'max-w-md',
}: SheetProps) {
  React.useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && open) {
        onOpenChange(false);
      }
    };
    window.addEventListener('keydown', handleKeyDown);
    return () => window.removeEventListener('keydown', handleKeyDown);
  }, [open, onOpenChange]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 overflow-hidden">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/60 backdrop-blur-sm transition-opacity"
        onClick={() => onOpenChange(false)}
      />

      <div className="fixed inset-y-0 right-0 flex max-w-full pl-10">
        <div
          className={cn(
            'relative w-screen border-l border-dark-700 bg-dark-900 shadow-2xl p-6 overflow-y-auto',
            width
          )}
        >
          <div className="flex items-center justify-between pb-4 border-b border-dark-800">
            <div>
              <h2 className="text-lg font-semibold text-white">{title}</h2>
              {description && <p className="text-xs text-slate-400 mt-0.5">{description}</p>}
            </div>
            <button
              onClick={() => onOpenChange(false)}
              className="rounded-lg p-1.5 text-slate-400 hover:text-white hover:bg-dark-800 transition-colors"
            >
              <X className="h-5 w-5" />
            </button>
          </div>

          <div className="mt-5">{children}</div>
        </div>
      </div>
    </div>
  );
}
