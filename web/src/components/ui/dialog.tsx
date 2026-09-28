import * as React from 'react';
import { X } from 'lucide-react';
import { cn } from './button';

interface DialogProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  title: string;
  description?: string;
  children: React.ReactNode;
  footer?: React.ReactNode;
  maxWidth?: string;
}

export function Dialog({
  open,
  onOpenChange,
  title,
  description,
  children,
  footer,
  maxWidth = 'max-w-lg',
}: DialogProps) {
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
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4">
      {/* Backdrop */}
      <div
        className="fixed inset-0 bg-black/70 backdrop-blur-sm transition-opacity"
        onClick={() => onOpenChange(false)}
      />

      {/* Content */}
      <div
        className={cn(
          'relative z-50 w-full overflow-hidden rounded-xl border border-dark-700 bg-dark-900 p-6 shadow-2xl shadow-black/60 transition-all sm:my-8',
          maxWidth
        )}
      >
        <div className="flex items-center justify-between pb-3 border-b border-dark-800">
          <div>
            <h3 className="text-lg font-semibold text-white leading-none">{title}</h3>
            {description && <p className="mt-1 text-xs text-slate-400">{description}</p>}
          </div>
          <button
            onClick={() => onOpenChange(false)}
            className="rounded-md p-1 text-slate-400 hover:text-white hover:bg-dark-800 transition-colors"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="py-4 text-slate-200">{children}</div>

        {footer && (
          <div className="flex justify-end gap-3 pt-3 border-t border-dark-800">
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}
