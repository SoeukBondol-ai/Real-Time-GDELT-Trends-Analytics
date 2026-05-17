import React, { useEffect, useMemo, useState } from "react";
import ReactDOM from "react-dom/client";
import { Activity, Database, Radio, Server, Clock, BarChart3, List } from "lucide-react";
import {
  Bar,
  BarChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
  Cell,
} from "recharts";
import "./styles.css";

type TrendItem = {
  keyword: string;
  source: string;
  mention_count: number;
  avg_sentiment: number;
  trend_score: number;
  window_start: string;
  window_end: string;
};

const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";
const WS_URL = import.meta.env.VITE_WS_URL || "ws://localhost:8000/ws/trends";

function formatTime(value: string): string {
  if (!value) return "--:--:--";
  const date = new Date(value);
  return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}

function sentimentLabel(value: number): string {
  if (value > 0.04) return "Positive";
  if (value < -0.04) return "Negative";
  return "Neutral";
}

function App() {
  const [trends, setTrends] = useState<TrendItem[]>([]);
  const [status, setStatus] = useState("connecting");
  const [lastUpdate, setLastUpdate] = useState<string>("");

  useEffect(() => {
    let socket: WebSocket | null = null;
    let stopped = false;

    async function fallbackFetch() {
      try {
        const response = await fetch(`${API_BASE_URL}/api/trends/latest?limit=20`);
        const data = await response.json();
        setTrends(data);
        setStatus("polling");
        setLastUpdate(new Date().toISOString());
      } catch {
        setStatus("offline");
      }
    }

    function connect() {
      socket = new WebSocket(WS_URL);
      socket.onopen = () => setStatus("live");
      socket.onmessage = (event) => {
        const data = JSON.parse(event.data);
        setTrends(data);
        setLastUpdate(new Date().toISOString());
      };
      socket.onerror = () => setStatus("error");
      socket.onclose = () => {
        if (!stopped) {
          setStatus("reconnecting");
          fallbackFetch();
          window.setTimeout(connect, 3000);
        }
      };
    }

    connect();
    return () => {
      stopped = true;
      if (socket) socket.close();
    };
  }, []);

  const chartData = useMemo(
    () =>
      trends.slice(0, 15).map((item) => ({
        name: item.keyword,
        score: Number(item.trend_score.toFixed(1)),
      })),
    [trends]
  );

  const totalVolume = trends.reduce((sum, item) => sum + item.mention_count, 0);
  const topKeyword = trends[0]?.keyword || "---";
  const topSource = trends[0]?.source || "---";

  return (
    <main className="app">
      <header className="topbar">
        <div>
          <p className="eyebrow">Data Pipeline Analytics</p>
          <h1>Trends Explorer</h1>
        </div>
        <div className="status-wrap">
          <span className="badge">Kafka / Spark / Timescale</span>
          <div className="badge" style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
            <div className={`status-dot ${status}`} />
            {status.toUpperCase()}
          </div>
        </div>
      </header>

      <section className="stats-grid">
        <article className="metric-card">
          <span>Top Keyword</span>
          <strong>{topKeyword}</strong>
        </article>
        <article className="metric-card">
          <span>Total Volume</span>
          <strong>{totalVolume.toLocaleString()}</strong>
        </article>
        <article className="metric-card">
          <span>Primary Source</span>
          <strong>{topSource}</strong>
        </article>
        <article className="metric-card">
          <span>Last Window</span>
          <strong>{formatTime(lastUpdate)}</strong>
        </article>
      </section>

      <section className="content-grid">
        <article className="panel">
          <div className="panel-header">
            <h2 style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <BarChart3 size={16} color="#0f766e" /> Trend Velocity
            </h2>
          </div>
          <div className="panel-body">
            {chartData.length === 0 ? (
              <div style={{ height: 400, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#667085', fontSize: 13 }}>
                Waiting for pipeline data...
              </div>
            ) : (
              <ResponsiveContainer width="100%" height={450}>
                <BarChart data={chartData} margin={{ top: 20, right: 30, left: 10, bottom: 100 }}>
                  <XAxis 
                    dataKey="name" 
                    axisLine={false} 
                    tickLine={false} 
                    fontSize={13} 
                    fontWeight={600}
                    tick={{ fill: "#111827" }}
                    angle={-45}
                    textAnchor="end"
                    interval={0}
                    height={80}
                  />
                  <YAxis 
                    axisLine={false} 
                    tickLine={false} 
                    fontSize={12} 
                    tick={{ fill: "#667085" }} 
                  />
                  <Tooltip 
                    cursor={{ fill: "#f0f2f5" }}
                    contentStyle={{ borderRadius: '8px', border: '1px solid #d9dee7', boxShadow: '0 4px 12px rgba(0,0,0,0.05)' }}
                  />
                  <Bar dataKey="score" radius={[4, 4, 0, 0]} fill="#0f766e" />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </article>

        <article className="panel">
          <div className="panel-header">
            <h2 style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
              <List size={16} color="#0f766e" /> Live Leaderboard
            </h2>
          </div>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Keyword</th>
                  <th>Source</th>
                  <th>Volume</th>
                  <th>Sentiment</th>
                  <th style={{ textAlign: 'right' }}>Score</th>
                </tr>
              </thead>
              <tbody>
                {trends.map((item) => (
                  <tr key={`${item.keyword}-${item.source}`}>
                    <td className="keyword-cell">{item.keyword}</td>
                    <td><span className={`source-tag ${item.source}`}>{item.source}</span></td>
                    <td>{item.mention_count}</td>
                    <td style={{ fontSize: 13 }}>{sentimentLabel(item.avg_sentiment)}</td>
                    <td style={{ textAlign: 'right', fontWeight: '800' }}>{item.trend_score.toFixed(1)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </article>
      </section>
    </main>
  );
}

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
