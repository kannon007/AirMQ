import * as React from 'react';
import { cva, type VariantProps } from 'class-variance-authority';
import { cn } from './button';

const badgeVariants = cva(
  'inline-flex items-center rounded-full border px-2.5 py-0.5 text-xs font-semibold transition-colors focus:outline-none focus:ring-2 focus:ring-brand-500 focus:ring-offset-2',
  {
    variants: {
      variant: {
        default: 'border-transparent bg-brand-500/20 text-brand-400 border-brand-500/30',
        secondary: 'border-transparent bg-dark-700 text-slate-300',
        outline: 'border-dark-600 text-slate-300',
        destructive: 'border-transparent bg-rose-500/20 text-rose-400 border-rose-500/30',
        // Transports
        tcp: 'bg-blue-500/15 text-blue-400 border-blue-500/30 font-mono',
        tls: 'bg-purple-500/15 text-purple-400 border-purple-500/30 font-mono',
        ws: 'bg-amber-500/15 text-amber-400 border-amber-500/30 font-mono',
        quic: 'bg-emerald-500/20 text-emerald-400 border-emerald-500/40 font-mono shadow-sm shadow-emerald-500/10',
        // Cluster health
        alive: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
        suspect: 'bg-amber-500/15 text-amber-400 border-amber-500/30',
        dead: 'bg-rose-500/15 text-rose-400 border-rose-500/30',
      },
    },
    defaultVariants: {
      variant: 'default',
    },
  }
);

export interface BadgeProps
  extends React.HTMLAttributes<HTMLDivElement>,
    VariantProps<typeof badgeVariants> {}

export function Badge({ className, variant, ...props }: BadgeProps) {
  return <div className={cn(badgeVariants({ variant }), className)} {...props} />;
}
