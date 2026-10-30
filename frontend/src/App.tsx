import { useEffect, useState } from 'react'

type Health = { status: string; database: boolean; queue: boolean; posts?: number; processed?: number; flushes?: number }
type Pub = { pub_key: string; name: string; article_count: number; distinct_entities: number }
type Ent = { word: string; count: number }
type Flush = { id: number; pub_key: string; name: string; worker_pid: number; posts: number; entities: number; flushed_at: string }
type PostRow = { id: string; publication: string; title: string; chars: number; received_at: string; processed_at: string | null; flush_id: number | null; latency_s: number | null }

const get = async <T,>(u: string): Promise<T> => { const r = await fetch(u); if (!r.ok) throw new Error(`${u}: ${r.status}`); return r.json() }
const t = (s: string) => new Date(s).toLocaleTimeString([], { hour12: false })

export default function App() {
  const [health, setHealth] = useState<Health>()
  const [pubs, setPubs] = useState<Pub[]>([])
  const [pub, setPub] = useState('')
  const [ents, setEnts] = useState<Ent[]>([])
  const [flushes, setFlushes] = useState<Flush[]>([])
  const [posts, setPosts] = useState<PostRow[]>([])
  const [err, setErr] = useState('')

  useEffect(() => {
    const load = () => Promise.all([
      get<Health>('/health').then(setHealth),
      get<Pub[]>('/api/publications').then(setPubs),
      get<Ent[]>(`/api/entities?limit=15${pub ? `&publication=${encodeURIComponent(pub)}` : ''}`).then(setEnts),
      get<Flush[]>('/api/flushes?limit=20').then(setFlushes),
      get<PostRow[]>('/api/posts?limit=40').then(setPosts),
    ]).then(() => setErr('')).catch(e => setErr(String(e)))
    load()
    const id = setInterval(load, 2000)
    return () => clearInterval(id)
  }, [pub])

  const max = Math.max(1, ...ents.map(e => e.count))
  const pending = (health?.posts ?? 0) - (health?.processed ?? 0)
  return (
    <div className="page">
      <header>
        <h1>Ingestion runs</h1>
        <p className="sub">FastAPI front-end → queue → spaCy NER workers → savers → PostgreSQL. Refreshes every 2 s.</p>
      </header>
      {err && <div className="error">{err}</div>}
      <section className="cards">
        <div className="card"><span>Status</span><b className={health?.status === 'ok' ? 'ok' : 'bad'}>{health?.status ?? '…'}</b><small>db {health?.database ? '✓' : '✗'} · queue {health?.queue ? '✓' : '✗'}</small></div>
        <div className="card"><span>Posts received</span><b>{health?.posts ?? 0}</b></div>
        <div className="card"><span>Processed</span><b>{health?.processed ?? 0}</b><small>{pending} pending</small></div>
        <div className="card"><span>Flushes</span><b>{health?.flushes ?? 0}</b></div>
      </section>
      <div className="grid">
        <section className="panel">
          <h2>Publications</h2>
          <table><thead><tr><th>Publication</th><th>Articles</th><th>Entities</th></tr></thead><tbody>
            {pubs.map(p => <tr key={p.pub_key} className={pub === p.pub_key ? 'sel' : ''} onClick={() => setPub(pub === p.pub_key ? '' : p.pub_key)}>
              <td>{p.name ?? p.pub_key}</td><td>{p.article_count}</td><td>{p.distinct_entities}</td></tr>)}
          </tbody></table>
          <small className="hint">Click a row to filter the entities chart.</small>
        </section>
        <section className="panel">
          <h2>Top entities {pub ? `· ${pubs.find(p => p.pub_key === pub)?.name ?? pub}` : '· all publications'}</h2>
          {ents.map(e => <div className="bar" key={e.word}><span className="label">{e.word}</span><span className="fill" style={{ width: `${(e.count / max) * 100}%` }} /><span className="n">{e.count}</span></div>)}
        </section>
      </div>
      <section className="panel">
        <h2>Flush runs (worker → saver batches)</h2>
        <table><thead><tr><th>#</th><th>Time</th><th>Publication</th><th>Worker PID</th><th>Posts</th><th>Entities</th></tr></thead><tbody>
          {flushes.map(f => <tr key={f.id}><td>{f.id}</td><td>{t(f.flushed_at)}</td><td>{f.name ?? f.pub_key}</td><td>{f.worker_pid}</td><td>{f.posts}</td><td>{f.entities}</td></tr>)}
        </tbody></table>
      </section>
      <section className="panel">
        <h2>Recent posts</h2>
        <table><thead><tr><th>Received</th><th>Publication</th><th>Title</th><th>Chars</th><th>Flush</th><th>Latency</th></tr></thead><tbody>
          {posts.map(p => <tr key={p.id}><td>{t(p.received_at)}</td><td>{p.publication}</td><td>{p.title}</td><td>{p.chars}</td>
            <td>{p.flush_id ?? <em>pending</em>}</td><td>{p.latency_s != null ? `${p.latency_s.toFixed(2)} s` : '–'}</td></tr>)}
        </tbody></table>
      </section>
      <footer>Sample text: English Wikipedia intro extracts (CC BY-SA 4.0); publication names are made up for the demo.</footer>
    </div>
  )
}
