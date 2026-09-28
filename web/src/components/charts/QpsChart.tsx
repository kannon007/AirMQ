import React, { useEffect, useRef } from 'react';

export interface DataPoint {
  time: string;
  inbound: number;
  outbound: number;
}

interface QpsChartProps {
  data: DataPoint[];
  height?: number;
}

export const QpsChart: React.FC<QpsChartProps> = ({ data, height = 240 }) => {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    // Handle high DPI
    const dpr = window.devicePixelRatio || 1;
    const rect = canvas.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    ctx.scale(dpr, dpr);

    const w = rect.width;
    const h = rect.height;
    ctx.clearRect(0, 0, w, h);

    if (data.length < 2) {
      ctx.fillStyle = '#64748b';
      ctx.font = '12px sans-serif';
      ctx.textAlign = 'center';
      ctx.fillText('Collecting real-time throughput metrics...', w / 2, h / 2);
      return;
    }

    const maxVal = Math.max(
      ...data.map((d) => Math.max(d.inbound, d.outbound, 10))
    ) * 1.2;

    const padLeft = 45;
    const padBottom = 25;
    const padTop = 15;
    const padRight = 15;

    const chartW = w - padLeft - padRight;
    const chartH = h - padTop - padBottom;

    // Draw horizontal grid lines
    ctx.strokeStyle = '#1e293b';
    ctx.lineWidth = 1;
    ctx.fillStyle = '#64748b';
    ctx.font = '10px monospace';
    ctx.textAlign = 'right';

    const gridLines = 4;
    for (let i = 0; i <= gridLines; i++) {
      const y = padTop + (chartH / gridLines) * i;
      ctx.beginPath();
      ctx.moveTo(padLeft, y);
      ctx.lineTo(w - padRight, y);
      ctx.stroke();

      const val = Math.round(maxVal - (maxVal / gridLines) * i);
      ctx.fillText(String(val), padLeft - 8, y + 3);
    }

    // Function to draw smooth line & gradient area
    const drawSeries = (
      values: number[],
      strokeColor: string,
      gradientStart: string
    ) => {
      const points: { x: number; y: number }[] = values.map((val, idx) => {
        const x = padLeft + (chartW / (values.length - 1)) * idx;
        const y = padTop + chartH - (val / maxVal) * chartH;
        return { x, y };
      });

      // Gradient area
      const grad = ctx.createLinearGradient(0, padTop, 0, padTop + chartH);
      grad.addColorStop(0, gradientStart);
      grad.addColorStop(1, 'rgba(15, 23, 42, 0)');

      ctx.beginPath();
      ctx.moveTo(points[0].x, padTop + chartH);
      points.forEach((p) => ctx.lineTo(p.x, p.y));
      ctx.lineTo(points[points.length - 1].x, padTop + chartH);
      ctx.closePath();
      ctx.fillStyle = grad;
      ctx.fill();

      // Stroke Line
      ctx.beginPath();
      ctx.moveTo(points[0].x, points[0].y);
      for (let i = 0; i < points.length - 1; i++) {
        const xc = (points[i].x + points[i + 1].x) / 2;
        const yc = (points[i].y + points[i + 1].y) / 2;
        ctx.quadraticCurveTo(points[i].x, points[i].y, xc, yc);
      }
      ctx.lineTo(points[points.length - 1].x, points[points.length - 1].y);
      ctx.strokeStyle = strokeColor;
      ctx.lineWidth = 2.5;
      ctx.stroke();

      // Draw latest point dot
      const last = points[points.length - 1];
      ctx.beginPath();
      ctx.arc(last.x, last.y, 4, 0, Math.PI * 2);
      ctx.fillStyle = strokeColor;
      ctx.fill();
      ctx.strokeStyle = '#0f172a';
      ctx.lineWidth = 2;
      ctx.stroke();
    };

    // Draw Inbound (Emerald #10b981)
    drawSeries(
      data.map((d) => d.inbound),
      '#10b981',
      'rgba(16, 185, 129, 0.25)'
    );

    // Draw Outbound (Cyan #06b6d4)
    drawSeries(
      data.map((d) => d.outbound),
      '#06b6d4',
      'rgba(6, 182, 212, 0.2)'
    );

    // Time ticks at bottom
    ctx.fillStyle = '#64748b';
    ctx.font = '10px monospace';
    ctx.textAlign = 'center';
    if (data.length > 0) {
      ctx.fillText(data[0].time, padLeft + 15, h - 6);
      const midIdx = Math.floor(data.length / 2);
      ctx.fillText(data[midIdx].time, padLeft + chartW / 2, h - 6);
      ctx.fillText(data[data.length - 1].time, w - padRight - 15, h - 6);
    }
  }, [data, height]);

  return (
    <div className="w-full relative">
      <div className="flex items-center justify-end gap-5 mb-2 text-xs">
        <div className="flex items-center gap-1.5 font-medium text-emerald-400">
          <span className="w-2.5 h-2.5 rounded-full bg-emerald-500 shadow-sm shadow-emerald-500/50" />
          <span>Inbound (Msg/s)</span>
        </div>
        <div className="flex items-center gap-1.5 font-medium text-cyan-400">
          <span className="w-2.5 h-2.5 rounded-full bg-cyan-500 shadow-sm shadow-cyan-500/50" />
          <span>Outbound (Msg/s)</span>
        </div>
      </div>
      <canvas
        ref={canvasRef}
        style={{ width: '100%', height: `${height}px` }}
        className="block"
      />
    </div>
  );
};
