import { useEffect, useRef, useState } from "react";
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from "recharts";

// Minimal, single-file dashboard that polls your FastAPI:
//  - GET /metrics  (every 1s)
//  - GET /alerts   (every 2s)
// Shows a trades/sec sparkline and the latest alerts.
//
// Configure API base with Vite env:
//   VITE_API_BASE=http://127.0.0.1:8000
// Fallback defaults to localhost if not set.

const API_BASE = import.meta.env.VITE_API_BASE || "http://127.0.0.1:8000";
const MAX_POINTS = 120; // ~2 minutes at 1s polling

function usePollingMetrics() {
  const [metrics, setMetrics] = useState(null);
  const [series, setSeries] = useState([]);
  const [status, setStatus] = useState("idle"); // idle | ok | error

  // keep a ref to avoid stale closure while pushing points
  const seriesRef = useRef([]);

  useEffect(() => {
    seriesRef.current = series;
  }, [series]);

  useEffect(() => {
    let cancelled = false;

    const fetchOnce = async () => {
      try {
        const res = await fetch(`${API_BASE}/metrics`, { cache: "no-store" });
        if (!res.ok) {
          // 404 when metrics not ready yet — surface but don't kill polling
          setStatus("error");
          return;
        }
        const data = await res.json();
        if (cancelled) return;
        setMetrics(data);
        setStatus("ok");

        const point = {
          t: data.last_updated_ms ? new Date(data.last_updated_ms) : new Date(),
          v: typeof data.trades_per_sec === "number" ? data.trades_per_sec : 0,
        };
        const next = [...seriesRef.current, point].slice(-MAX_POINTS);
        setSeries(next);
      } catch {
        if (!cancelled) setStatus("error");
      }
    };

    fetchOnce();
    const id = setInterval(fetchOnce, 1000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);

  return { metrics, series, status };
}

function usePollingAlerts() {
  const [alerts, setAlerts] = useState([]);
  useEffect(() => {
    let cancelled = false;
    const fetchAlerts = async () => {
      try {
        const res = await fetch(`${API_BASE}/alerts?limit=50`, { cache: "no-store" });
        if (res.ok) {
          const data = await res.json();
          if (!cancelled) setAlerts(Array.isArray(data) ? data : []);
        }
      } catch {
        // ignore transient errors
      }
    };
    fetchAlerts();
    const id = setInterval(fetchAlerts, 2000);
    return () => {
      cancelled = true;
      clearInterval(id);
    };
  }, []);
  return alerts;
}

function Stat({ label, value, hint, danger }) {
  return (
    <div className="rounded-2xl shadow p-4 bg-white/80 border">
      <div className="text-sm text-gray-500">{label}</div>
      <div className="text-2xl font-semibold mt-1 flex items-baseline gap-2">
        <span className={danger ? "text-red-600" : "text-gray-900"}>{value}</span>
        {hint ? <span className="text-xs text-gray-500">{hint}</span> : null}
      </div>
    </div>
  );
}

function Sparkline({ data }) {
  const chartData = data.map(d => ({ ts: d.t, trades: Math.max(0, Math.round(d.v)) }));

  return (
    <div
      style={{
        height: 260,           // <- makes Recharts render
        width: "100%",
        padding: 12,
        border: "1px solid #e5e7eb",
        borderRadius: 16,
        background: "rgba(255,255,255,0.9)",
        marginTop: 12
      }}
    >
      <div style={{ fontSize: 12, color: "#475569", marginBottom: 8 }}>
        Trades per second (live)
      </div>
      <ResponsiveContainer width="100%" height="100%">
        <LineChart data={chartData} margin={{ top: 8, right: 12, bottom: 8, left: 0 }}>
          <CartesianGrid strokeDasharray="3 3" />
          <XAxis dataKey="ts" tickFormatter={(v)=>new Date(v).toLocaleTimeString()} minTickGap={24}/>
          <YAxis allowDecimals={false} width={36}/>
          <Tooltip labelFormatter={(v)=>new Date(v).toLocaleTimeString()}
                   formatter={(value)=>[value, "trades/sec"]}/>
          <Line type="monotone" dataKey="trades" dot={false} strokeWidth={2} isAnimationActive={false}/>
        </LineChart>
      </ResponsiveContainer>
    </div>
  );
}


function AlertsList({ alerts }) {
  if (!alerts?.length) {
    return (
      <div className="rounded-2xl bg-white/80 border shadow p-4 text-sm text-gray-500">
        No recent alerts.
      </div>
    );
  }
  return (
    <div className="rounded-2xl bg-white/80 border shadow divide-y">
      {alerts.map((a, idx) => (
        <div key={`${a.ts_ms}-${idx}`} className="p-3 flex items-center justify-between">
          <div>
            <div className="text-sm font-medium">
              Spike detected
              <span className="ml-2 inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium bg-red-100 text-red-800">
                z {a.z}
              </span>
            </div>
            <div className="text-xs text-gray-500">
              {new Date(a.ts_ms).toLocaleTimeString()} · count={a.count}
            </div>
          </div>
          <div className="text-sm font-semibold">{a.count} tps</div>
        </div>
      ))}
    </div>
  );
}

export default function App() {
  const { metrics, series, status } = usePollingMetrics();
  const alerts = usePollingAlerts();

  const tps = metrics?.trades_per_sec ?? 0;
  const z = metrics?.z ?? 0;
  const isSpike = !!metrics?.is_spike;
  const last = metrics?.last_updated_ms ? new Date(metrics.last_updated_ms).toLocaleTimeString() : "—";

  return (
    <div className="min-h-screen w-full bg-gradient-to-b from-slate-50 to-slate-100 text-slate-900 p-6">
      <div className="max-w-6xl mx-auto space-y-6">
        <header className="flex items-end justify-between">
          <div>
            <h1 className="text-2xl font-bold">SecureDataOps — Live Trades</h1>
            <p className="text-sm text-gray-600">API: {API_BASE}</p>
          </div>
          <div className="text-xs">
            <span
              className={
                "inline-flex items-center gap-1 px-2 py-1 rounded-full border " +
                (status === "ok"
                  ? "bg-green-50 text-green-700 border-green-200"
                  : status === "error"
                  ? "bg-amber-50 text-amber-700 border-amber-200"
                  : "bg-gray-50 text-gray-700 border-gray-200")
              }
            >
              <span className="w-2 h-2 rounded-full bg-current inline-block" />
              {status === "ok" ? "connected" : status === "error" ? "waiting for metrics…" : "idle"}
            </span>
          </div>
        </header>

        {/* Stats */}
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <Stat label="Trades / sec" value={tps} hint={`as of ${last}`} danger={isSpike} />
          <Stat label="z-score" value={z} hint={isSpike ? "spike" : "normal"} danger={isSpike} />
          <Stat label="Points" value={series.length} hint={`/ ${MAX_POINTS}`} />
        </div>

        {/* Sparkline */}
        <Sparkline data={series} />

        {/* Alerts */}
        <section className="space-y-2">
          <h2 className="text-lg font-semibold">Recent Alerts</h2>
          <AlertsList alerts={alerts} />
        </section>

        <footer className="text-xs text-gray-500 pt-6">
          Polling /metrics every 1s and /alerts every 2s. Configure with VITE_API_BASE.
        </footer>
      </div>
    </div>
  );
}
