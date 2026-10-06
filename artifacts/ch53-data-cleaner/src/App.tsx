import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { QueryClient, QueryClientProvider, useQueryClient } from '@tanstack/react-query';
import {
  Activity, AlertCircle, ArrowDownToLine, ArrowRight, AudioLines, Check,
  CheckCircle2, ChevronLeft, ChevronRight, CircleHelp, Clock3,
  FileJson2, FileUp, Filter, Gauge, HardDrive, History, Layers3, LoaderCircle,
  LockKeyhole, Plane, RefreshCw, Search, ShieldCheck, SlidersHorizontal,
  Sparkles, UploadCloud, X,
} from 'lucide-react';
import {
  getGetDataSessionQueryKey, getGetFftAuditQueryKey, getGetSessionAnalyticsQueryKey,
  getGetSessionFlightsQueryKey, useApplyCleanup, useCloseDataSession,
  useCreateDataSession, useExportCleanedData, useGetDataSession, useGetFftAudit,
  useGetSessionAnalytics, useGetSessionFlights, usePreviewCleanup,
  type CleanupOptions, type CleanupPreview, type DataSession,
} from '@workspace/api-client-react';

const queryClient = new QueryClient();
const countFields = [
  ['aircraft', 'Aircraft'], ['tests', 'Tests'], ['flights', 'Flights'],
  ['balanceReadings', 'Balance readings'], ['trackReadings', 'Track readings'],
  ['fftCaptures', 'FFT captures'], ['fftSamples', 'FFT samples'],
] as const;
const cleanupChoices: { key: keyof CleanupOptions; title: string; detail: string }[] = [
  { key: 'removeEmptyFlights', title: 'Remove empty flights', detail: 'Flights with no retained reading data' },
  { key: 'mergeKnownTailVariants', title: 'Merge known tail variants', detail: 'Documented tail-number variants' },
  { key: 'removeJunkAircraft', title: 'Remove junk aircraft records', detail: 'Known non-aircraft entries' },
  { key: 'applyDateCorrections', title: 'Apply date corrections', detail: 'Documented date corrections' },
  { key: 'removeTargetedTests', title: 'Remove targeted tests', detail: 'Tests explicitly covered by cleanup rules' },
  { key: 'mergeEvidenceBasedChains', title: 'Merge evidence-based chains', detail: 'Chains supported by matching evidence' },
];
const initialOptions: CleanupOptions = {
  removeEmptyFlights: true,
  mergeKnownTailVariants: true,
  removeJunkAircraft: true,
  applyDateCorrections: true,
  removeTargetedTests: true,
  mergeEvidenceBasedChains: true,
};

type LocalEvent = { label: string; time: string; tone?: 'success' | 'warning' };
const nowLabel = () => new Intl.DateTimeFormat('en-GB', { hour: '2-digit', minute: '2-digit' }).format(new Date());
const prettyBytes = (bytes: number) => bytes < 1024 * 1024 ? `${(bytes / 1024).toFixed(0)} KB` : `${(bytes / 1024 / 1024).toFixed(1)} MB`;
const friendlyError = (error: unknown) => error instanceof Error ? error.message : 'The request could not be completed. Try again.';

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Workbench />
    </QueryClientProvider>
  );
}

function Workbench() {
  const qc = useQueryClient();
  const [sessionId, setSessionId] = useState(() => {
    const fromUrl = new URLSearchParams(window.location.search).get('sessionId');
    return fromUrl || window.localStorage.getItem('ch53-session-id') || '';
  });
  const [file, setFile] = useState<File | null>(null);
  const [dragActive, setDragActive] = useState(false);
  const [options, setOptions] = useState<CleanupOptions>(initialOptions);
  const [preview, setPreview] = useState<CleanupPreview | null>(null);
  const [search, setSearch] = useState('');
  const [tailFilter, setTailFilter] = useState('');
  const [conditionFilter, setConditionFilter] = useState('');
  const [flightPage, setFlightPage] = useState(1);
  const [expandedFlight, setExpandedFlight] = useState('');
  const [activity, setActivity] = useState<LocalEvent[]>([]);
  const [exportRequested, setExportRequested] = useState(false);
  const fileInput = useRef<HTMLInputElement>(null);

  const sessionQuery = useGetDataSession({ sessionId: sessionId || '' }, {
    query: { enabled: !!sessionId, queryKey: getGetDataSessionQueryKey({ sessionId: sessionId || '' }) },
  });
  const session = sessionQuery.data;
  const flightsParams = useMemo(() => ({
    sessionId: sessionId || '', page: flightPage, limit: 6,
    ...(search.trim() ? { search: search.trim() } : {}),
    ...(tailFilter.trim() ? { tailNumber: tailFilter.trim() } : {}),
    ...(conditionFilter ? { condition: conditionFilter } : {}),
  }), [sessionId, flightPage, search, tailFilter, conditionFilter]);
  const flightsQuery = useGetSessionFlights(flightsParams, {
    query: { enabled: !!sessionId, queryKey: getGetSessionFlightsQueryKey(flightsParams) },
  });
  const analyticsQuery = useGetSessionAnalytics(sessionId || '', {
    query: { enabled: !!sessionId, queryKey: getGetSessionAnalyticsQueryKey(sessionId || '') },
  });
  const fftQuery = useGetFftAudit(sessionId || '', {
    query: { enabled: !!sessionId, queryKey: getGetFftAuditQueryKey(sessionId || '') },
  });
  const exportQuery = useExportCleanedData(sessionId || '', {
    query: { enabled: !!sessionId && exportRequested && !!session?.cleaned, queryKey: ['export-cleaned-data', sessionId] },
  });
  const createSession = useCreateDataSession();
  const previewCleanup = usePreviewCleanup();
  const applyCleanup = useApplyCleanup();
  const closeSession = useCloseDataSession();

  const addActivity = (label: string, tone?: LocalEvent['tone']) =>
    setActivity((events) => [{ label, time: nowLabel(), tone }, ...events].slice(0, 6));

  useEffect(() => {
    if (!sessionId) return;
    const closeTemporarySession = () => {
      closeSession.mutate({ sessionId });
      window.localStorage.removeItem('ch53-session-id');
    };
    window.addEventListener('pagehide', closeTemporarySession);
    return () => window.removeEventListener('pagehide', closeTemporarySession);
  }, [sessionId]);

  const acceptFile = (picked: File | undefined) => {
    if (!picked) return;
    if (!picked.name.toLowerCase().endsWith('.json') && picked.type !== 'application/json') {
      setFile(null);
      addActivity('Import rejected · select a JSON file', 'warning');
      return;
    }
    setFile(picked);
  };
  const startUpload = () => {
    if (!file || createSession.isPending) return;
    createSession.mutate({ data: file }, {
      onSuccess: (created: DataSession) => {
        if (sessionId && sessionId !== created.sessionId) closeSession.mutate({ sessionId });
        setSessionId(created.sessionId);
        window.localStorage.setItem('ch53-session-id', created.sessionId);
        window.history.replaceState({}, '', `/?sessionId=${encodeURIComponent(created.sessionId)}`);
        setPreview(null);
        setExportRequested(false);
        setFlightPage(1);
        addActivity(`Imported ${created.filename}`, 'success');
        qc.setQueryData(getGetDataSessionQueryKey({ sessionId: created.sessionId }), created);
      },
      onError: () => addActivity('Import failed · original file remains unchanged', 'warning'),
    });
  };
  const runPreview = () => {
    if (!sessionId) return;
    setPreview(null);
    previewCleanup.mutate({ sessionId, data: options }, {
      onSuccess: (result) => {
        setPreview(result);
        addActivity('Cleanup proposal generated · no changes applied');
      },
      onError: () => addActivity('Cleanup preview failed', 'warning'),
    });
  };
  const applyProposal = () => {
    if (!sessionId || !preview) return;
    if (preview.fftSamplesChanged !== 0) return;
    if (!window.confirm('Apply this reviewed cleanup proposal to the temporary session? FFT samples will remain unchanged.')) return;
    applyCleanup.mutate({ sessionId }, {
      onSuccess: (updated) => {
        qc.setQueryData(getGetDataSessionQueryKey({ sessionId }), updated);
        void qc.invalidateQueries({ queryKey: getGetSessionAnalyticsQueryKey(sessionId) });
        void qc.invalidateQueries({ queryKey: getGetFftAuditQueryKey(sessionId) });
        void qc.invalidateQueries({ queryKey: getGetSessionFlightsQueryKey(flightsParams) });
        addActivity('Approved cleanup applied · FFT samples preserved', 'success');
        setExportRequested(false);
      },
      onError: () => addActivity('Apply failed · session remains available', 'warning'),
    });
  };
  const clearSession = () => {
    if (!sessionId) return;
    if (!window.confirm('Close this temporary session? Its imported data will be removed.')) return;
    closeSession.mutate({ sessionId }, {
      onSuccess: () => {
        window.localStorage.removeItem('ch53-session-id');
        window.history.replaceState({}, '', '/');
        setSessionId('');
        setFile(null);
        setPreview(null);
        setExportRequested(false);
        addActivity('Temporary session closed', 'success');
      },
      onError: () => addActivity('Session could not be closed', 'warning'),
    });
  };
  const beginExport = () => {
    if (!session?.cleaned) return;
    setExportRequested(true);
    void exportQuery.refetch();
    addActivity('Original-schema export requested');
  };

  const counts = session?.counts;
  const analytics = analyticsQuery.data;
  const flights = flightsQuery.data;
  const fft = fftQuery.data;
  const shownEvents = [...activity, ...(session?.log ?? []).map((label) => ({ label, time: 'API' }))].slice(0, 7);

  return (
    <div className="min-h-[100dvh] bg-[#f1eee5] text-[#202c35]">
      <aside className="fixed inset-y-0 left-0 z-20 hidden w-[246px] flex-col bg-[#172832] text-[#e9e8df] lg:flex">
        <div className="flex h-[76px] items-center gap-3 border-b border-white/10 px-6">
          <div className="relative flex h-10 w-10 items-center justify-center rounded-lg bg-[#d5a348] text-[#172832]">
            <Plane size={20} strokeWidth={2.2} />
            <span className="absolute -bottom-1 -right-1 h-2.5 w-2.5 rounded-full border-2 border-[#172832] bg-[#89b09b]" />
          </div>
          <div>
            <div className="text-[12px] font-extrabold tracking-[.14em] text-[#f5f2e9]">ROTOR / RECORDS</div>
            <div className="mt-0.5 font-mono text-[9px] tracking-[.16em] text-[#9eb0b2]">CH-53G MAINTENANCE</div>
          </div>
        </div>
        <div className="px-4 pt-7">
          <div className="mb-3 px-3 font-mono text-[9px] uppercase tracking-[.2em] text-[#809398]">Workbench</div>
          <div className="flex items-center gap-3 rounded-md bg-white/[.08] px-3 py-3 text-[12px] font-semibold text-[#f5f2e9]">
            <Layers3 size={15} className="text-[#e0b458]" /> Session cleaner
            <span className="ml-auto h-1.5 w-1.5 rounded-full bg-[#e0b458]" />
          </div>
          <a href="/data-app/" className="mt-1 flex items-center gap-3 rounded-md px-3 py-3 text-[12px] font-medium text-[#adbdbe] transition hover:bg-white/[.06] hover:text-white" data-testid="link-analytics">
            <Gauge size={15} /> Analytics workspace <ArrowRight className="ml-auto" size={13} />
          </a>
        </div>
        <div className="mx-6 mt-auto mb-5 rounded-lg border border-white/10 bg-white/[.035] p-4">
          <div className="flex items-center gap-2 font-mono text-[9px] uppercase tracking-[.15em] text-[#b9c9c5]"><LockKeyhole size={12} /> Temporary by design</div>
          <p className="mt-2 text-[11px] leading-[1.65] text-[#91a3a5]">Session data is held only for the review window. Export before closing.</p>
          <div className="mt-4 flex items-center gap-2 border-t border-white/10 pt-3 font-mono text-[9px] text-[#d3b166]">
            <span className="h-1.5 w-1.5 rounded-full bg-[#d3b166]" /> FFT samples locked
          </div>
        </div>
        <div className="border-t border-white/10 px-6 py-4 font-mono text-[9px] tracking-[.12em] text-[#819398]">DATA QUALITY CELL · 01</div>
      </aside>

      <main className="min-h-[100dvh] lg:ml-[246px]">
        <header className="flex min-h-[76px] items-center justify-between border-b border-[#dcd7ca] bg-[#f7f5ed] px-5 sm:px-8">
          <div className="flex items-center gap-3">
            <div className="lg:hidden flex h-9 w-9 items-center justify-center rounded-md bg-[#172832] text-[#e0b458]"><Plane size={17} /></div>
            <div>
              <div className="font-mono text-[9px] uppercase tracking-[.16em] text-[#758087]">Data integrity / rotor systems</div>
              <div className="mt-1 text-[13px] font-bold tracking-[-.02em]">Record cleaning station</div>
            </div>
          </div>
          <div className="flex items-center gap-3">
            <div className="hidden items-center gap-2 rounded-full border border-[#d8d4c9] bg-[#fbfaf5] px-3 py-1.5 sm:flex">
              <span className={`h-1.5 w-1.5 rounded-full ${session ? 'bg-[#56846c]' : 'bg-[#aaa99f]'}`} />
              <span className="font-mono text-[9px] uppercase tracking-[.1em] text-[#626d70]">{session ? 'Session active' : 'No active session'}</span>
            </div>
            <a href="/data-app/" className="flex items-center gap-2 rounded-md bg-[#e5e1d6] px-3 py-2 text-[11px] font-bold text-[#34464c] hover:bg-[#dcd7ca] sm:hidden" data-testid="link-analytics-mobile">Analytics <ArrowRight size={13} /></a>
            {session && <button onClick={clearSession} disabled={closeSession.isPending} className="flex items-center gap-2 rounded-md border border-[#d8d4c9] px-3 py-2 text-[11px] font-semibold text-[#687176] hover:border-[#b2a99a] hover:text-[#263940] disabled:opacity-50" data-testid="button-close-session"><X size={13} /> Close session</button>}
          </div>
        </header>

        <div className="mx-auto max-w-[1390px] px-4 pb-12 pt-7 sm:px-8 lg:px-10">
          <div className="mb-7 flex flex-col justify-between gap-5 md:flex-row md:items-end">
            <div className="animate-enter">
              <div className="mb-2 flex items-center gap-2 font-mono text-[9px] uppercase tracking-[.2em] text-[#758087]"><span className="h-px w-6 bg-[#c0933f]" /> Rotor-track &amp; balance data</div>
              <h1 className="text-[29px] font-extrabold leading-[1.1] tracking-[-.055em] text-[#1d2e37] sm:text-[38px]">Clean the record.<br className="sm:hidden" /> Keep the evidence.</h1>
              <p className="mt-2 max-w-[620px] text-[12px] leading-6 text-[#6d7778]">Review documented cleanup proposals against the source dataset. Every change is previewed before it is applied.</p>
            </div>
            <div className="flex items-center gap-2 self-start rounded-md border border-[#d9d4c7] bg-[#f9f7f0] px-3 py-2 font-mono text-[9px] uppercase tracking-[.1em] text-[#677579] md:self-auto">
              <ShieldCheck size={14} className="text-[#3e7969]" /> Review-first workflow
            </div>
          </div>

          {!sessionId ? (
            <section className="animate-enter delay-1 grid gap-5 xl:grid-cols-[minmax(0,1.48fr)_minmax(270px,.72fr)]">
              <div className="overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5] shadow-[0_12px_34px_rgba(39,47,47,.045)]">
                <div className="flex items-center justify-between border-b border-[#e3ded2] px-5 py-4 sm:px-7">
                  <div className="flex items-center gap-3"><span className="flex h-8 w-8 items-center justify-center rounded-md bg-[#dce9e5] text-[#2f6c63]"><FileUp size={16} /></span><div><div className="text-[12px] font-bold">Start a temporary session</div><div className="mt-0.5 font-mono text-[9px] text-[#818887]">SOURCE FILE · JSON</div></div></div>
                  <span className="font-mono text-[9px] tracking-[.1em] text-[#9a9b91]">01 / IMPORT</span>
                </div>
                <div className="p-5 sm:p-7">
                  <input ref={fileInput} type="file" accept=".json,application/json" className="sr-only" onChange={(event) => acceptFile(event.target.files?.[0])} data-testid="input-data-file" />
                  <button
                    type="button"
                    onClick={() => fileInput.current?.click()}
                    onDragOver={(event) => { event.preventDefault(); setDragActive(true); }}
                    onDragLeave={() => setDragActive(false)}
                    onDrop={(event) => { event.preventDefault(); setDragActive(false); acceptFile(event.dataTransfer.files?.[0]); }}
                    className={`group w-full rounded-lg border border-dashed px-5 py-10 text-left transition sm:px-9 ${dragActive ? 'border-[#2f756b] bg-[#e9f0eb]' : 'border-[#bfbcae] bg-[#f6f4ec] hover:border-[#56847c] hover:bg-[#f0f1e9]'}`}
                    data-testid="dropzone-data-file"
                  >
                    <div className="mx-auto flex max-w-[500px] flex-col items-center text-center">
                      <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-xl bg-[#e2e8e1] text-[#386c62] group-hover:bg-[#d5e3dc]"><UploadCloud size={22} /></div>
                      <div className="text-[14px] font-bold text-[#283b42]">{file ? file.name : 'Drop a dataset here'}</div>
                      <div className="mt-1.5 text-[11px] text-[#7b8583]">{file ? `${prettyBytes(file.size)} · ready to transfer` : 'or choose a file from this workstation'}</div>
                      <span className="mt-5 rounded border border-[#d4d2c6] bg-[#fbfaf5] px-3 py-1.5 font-mono text-[9px] uppercase tracking-[.12em] text-[#586967]">JSON · large files accepted</span>
                    </div>
                  </button>
                  {createSession.isPending && <div className="mt-4 rounded-md border border-[#d7dfd7] bg-[#eff3ed] p-3" role="status" data-testid="status-uploading"><div className="flex items-center justify-between font-mono text-[9px] uppercase tracking-[.1em] text-[#527269]"><span className="flex items-center gap-2"><LoaderCircle size={13} className="animate-spin" /> Transferring binary file</span><span>in progress</span></div><div className="mt-2 h-1 overflow-hidden rounded-full bg-[#d7dfd7]"><div className="upload-indicator h-full w-1/3 rounded-full bg-[#3b786b]" /></div></div>}
                  {createSession.isError && <InlineError text={friendlyError(createSession.error)} onRetry={startUpload} />}
                  {!file && !createSession.isPending && <div className="mt-4 flex items-start gap-2 text-[10px] leading-5 text-[#858b86]"><CircleHelp size={13} className="mt-0.5 shrink-0" /> The file is sent as a binary stream; it is not read into a large browser-side string.</div>}
                  <div className="mt-5 flex flex-col gap-3 border-t border-[#e4dfd3] pt-5 sm:flex-row sm:items-center sm:justify-between">
                    <div className="flex items-center gap-2 text-[10px] text-[#777f7d]"><LockKeyhole size={13} className="text-[#567b6e]" /> Temporary working copy · original schema retained</div>
                    <button onClick={startUpload} disabled={!file || createSession.isPending} className="inline-flex items-center justify-center gap-2 rounded-md bg-[#1f5960] px-4 py-2.5 text-[11px] font-bold text-[#f4f2e7] transition hover:bg-[#17484f] disabled:cursor-not-allowed disabled:opacity-40" data-testid="button-import">
                      {createSession.isPending ? <LoaderCircle size={14} className="animate-spin" /> : <ArrowRight size={14} />} {createSession.isPending ? 'Importing…' : 'Import dataset'}
                    </button>
                  </div>
                </div>
              </div>
              <div className="flex flex-col gap-4">
                <div className="technical-grid relative overflow-hidden rounded-xl bg-[#1b303a] p-6 text-[#e9e8df] sm:p-7">
                  <div className="absolute -right-12 -top-14 h-48 w-48 rounded-full border border-[#d6aa5b]/20" /><div className="absolute -right-4 -top-6 h-32 w-32 rounded-full border border-[#d6aa5b]/15" />
                  <div className="relative">
                    <div className="font-mono text-[9px] uppercase tracking-[.18em] text-[#cfa854]">Control protocol / 03 steps</div>
                    <div className="mt-6 space-y-5">
                      {[
                        ['01', 'Import', 'Open an isolated review session'],
                        ['02', 'Inspect', 'Preview rules and retain FFT evidence'],
                        ['03', 'Release', 'Apply only after review, then export'],
                      ].map(([number, title, detail]) => <div key={number} className="flex gap-4"><span className="font-mono text-[10px] text-[#d0a951]">{number}</span><div><div className="text-[12px] font-bold">{title}</div><div className="mt-1 text-[10px] leading-4 text-[#a6b5b3]">{detail}</div></div></div>)}
                    </div>
                    <div className="mt-7 border-t border-white/10 pt-4 font-mono text-[9px] leading-5 text-[#a5b2b0]">PROCESSING NOTE<br /><span className="text-[#d8c08c]">FFT samples are protected from cleanup.</span></div>
                  </div>
                </div>
                <ActivityPanel events={shownEvents} />
              </div>
            </section>
          ) : sessionQuery.isLoading ? (
            <div className="grid gap-4 xl:grid-cols-[1.4fr_.8fr]">
              <div className="skeleton h-56 rounded-xl" /><div className="skeleton h-56 rounded-xl" /><div className="skeleton h-72 rounded-xl xl:col-span-2" />
            </div>
          ) : sessionQuery.isError || !session ? (
            <div className="rounded-xl border border-[#d8d2c5] bg-[#fbfaf5] p-8 text-center" data-testid="status-session-error">
              <AlertCircle className="mx-auto text-[#a3574d]" size={28} /><h2 className="mt-3 text-[15px] font-bold">Session unavailable</h2>
              <p className="mt-2 text-[11px] text-[#77807e]">{friendlyError(sessionQuery.error)}</p>
              <button onClick={() => void sessionQuery.refetch()} className="mt-5 inline-flex items-center gap-2 rounded bg-[#1f5960] px-4 py-2 text-[11px] font-bold text-white" data-testid="button-retry-session"><RefreshCw size={13} /> Retry session</button>
            </div>
          ) : (
            <>
              <section className="animate-enter delay-1 grid gap-4 xl:grid-cols-[minmax(0,1.34fr)_minmax(330px,.86fr)]">
                <div className="rounded-xl border border-[#d8d2c5] bg-[#fbfaf5] shadow-[0_12px_34px_rgba(39,47,47,.035)]">
                  <div className="flex flex-wrap items-center justify-between gap-3 border-b border-[#e2ddd1] px-5 py-4 sm:px-6">
                    <div className="flex items-center gap-3">
                      <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-[#dce9e5] text-[#2f6c63]"><FileJson2 size={17} /></span>
                      <div><div className="max-w-[260px] truncate text-[12px] font-bold" data-testid="text-session-filename">{session.filename}</div><div className="mt-1 flex gap-2 font-mono text-[9px] text-[#828986]"><span>{prettyBytes(session.bytes)}</span><span>·</span><span>Imported {new Date(session.importedAt).toLocaleString()}</span></div></div>
                    </div>
                    <div className={`rounded-full px-2.5 py-1 font-mono text-[9px] uppercase tracking-[.08em] ${session.cleaned ? 'bg-[#e2ece2] text-[#477354]' : 'bg-[#f1ead5] text-[#8c6d2e]'}`} data-testid="status-cleanup">{session.cleaned ? 'Cleaned copy ready' : 'Source snapshot'}</div>
                  </div>
                  <div className="grid grid-cols-2 gap-px bg-[#e5e0d5] sm:grid-cols-4">
                    {countFields.slice(0, 4).map(([key, label]) => <CountCell key={key} label={label} value={counts?.[key] ?? 0} />)}
                  </div>
                  <div className="flex flex-wrap items-center justify-between gap-3 px-5 py-3.5 sm:px-6">
                    <div className="flex items-center gap-2 text-[10px] text-[#697775]"><Clock3 size={13} className="text-[#a77a31}" /> Session expires in <strong className="font-mono text-[#344b4d]">{formatDuration(session.expiresInSeconds)}</strong></div>
                    <a href={`/data-app/?sessionId=${encodeURIComponent(session.sessionId)}`} className="inline-flex items-center gap-2 text-[10px] font-bold text-[#24646a] hover:text-[#17484f]" data-testid="link-session-analytics">Open session analytics <ArrowRight size={13} /></a>
                  </div>
                </div>
                <div className="grid grid-cols-3 gap-px overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#e1dbce]">
                  {countFields.slice(4).map(([key, label]) => <CountCell key={key} label={label} value={counts?.[key] ?? 0} compact />)}
                  <div className="col-span-3 flex items-center justify-between bg-[#f8f6ee] px-4 py-3">
                    <div className="flex items-center gap-2 text-[10px] font-semibold text-[#4c625c]"><ShieldCheck size={14} /> FFT evidence protection</div>
                    <span className="font-mono text-[9px] uppercase tracking-[.08em] text-[#54806d]">Read-only</span>
                  </div>
                </div>
              </section>

              <section className="animate-enter delay-2 mt-5 grid items-start gap-5 xl:grid-cols-[minmax(0,1.15fr)_minmax(340px,.85fr)]">
                <div className="overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5]">
                  <PanelHeading number="02" icon={<SlidersHorizontal size={15} />} title="Cleanup proposal" tag="Preview before apply" />
                  <div className="divide-y divide-[#e7e2d7] px-5 sm:px-6">
                    {cleanupChoices.map((choice) => <label key={choice.key} className="flex cursor-pointer items-center justify-between gap-4 py-3.5" data-testid={`option-${choice.key}`}>
                      <span><span className="block text-[11px] font-semibold text-[#36484b]">{choice.title}</span><span className="mt-1 block text-[9px] text-[#858c88]">{choice.detail}</span></span>
                      <input type="checkbox" checked={options[choice.key]} onChange={(event) => { setOptions((current) => ({ ...current, [choice.key]: event.target.checked })); setPreview(null); }} className="h-4 w-4 shrink-0 accent-[#28665f]" aria-label={choice.title} />
                    </label>)}
                  </div>
                  <div className="flex flex-col gap-3 border-t border-[#e3ded3] bg-[#f7f5ed] px-5 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-6">
                    <p className="max-w-[230px] text-[9px] leading-4 text-[#7d8582]">Preview runs documented rules only. No records change until you apply the reviewed plan.</p>
                    <button onClick={runPreview} disabled={previewCleanup.isPending} className="inline-flex items-center justify-center gap-2 rounded-md bg-[#234e52] px-4 py-2.5 text-[10px] font-bold text-[#f7f5ed] hover:bg-[#173f43] disabled:opacity-50" data-testid="button-preview">
                      {previewCleanup.isPending ? <LoaderCircle size={13} className="animate-spin" /> : <Sparkles size={13} />} {previewCleanup.isPending ? 'Calculating preview…' : 'Generate preview'}
                    </button>
                  </div>
                  {previewCleanup.isError && <div className="px-5 pb-4"><InlineError text={friendlyError(previewCleanup.error)} onRetry={runPreview} /></div>}
                  {preview && <div className="border-t border-[#ded8ca] px-5 py-5 sm:px-6" data-testid="panel-cleanup-preview">
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div><div className="font-mono text-[9px] uppercase tracking-[.13em] text-[#77817e]">Proposal review</div><div className="mt-1 text-[12px] font-bold">Before <ArrowRight className="mx-1 inline" size={12} /> After</div></div>
                      <div className={`flex items-center gap-1.5 rounded px-2.5 py-1 font-mono text-[9px] ${preview.fftSamplesChanged === 0 ? 'bg-[#e4ede4] text-[#467256]' : 'bg-[#f5e4df] text-[#9c4b41]'}`}><AudioLines size={12} /> FFT delta: {preview.fftSamplesChanged}</div>
                    </div>
                    <div className="mt-3 overflow-x-auto rounded-md border border-[#e4dfd3]">
                      <table className="w-full min-w-[410px] text-left text-[9px]"><thead className="bg-[#f0eee6] font-mono uppercase text-[#78817d]"><tr><th className="px-3 py-2">Record type</th><th className="px-3 py-2 text-right">Before</th><th className="px-3 py-2 text-right">After</th><th className="px-3 py-2 text-right">Δ</th></tr></thead><tbody className="divide-y divide-[#ece7db]">{countFields.map(([key, label]) => {
                        const before = preview.before[key]; const after = preview.after[key];
                        return <tr key={key}><td className="px-3 py-2 text-[#52605e]">{label}</td><td className="px-3 py-2 text-right font-mono">{before.toLocaleString()}</td><td className="px-3 py-2 text-right font-mono">{after.toLocaleString()}</td><td className={`px-3 py-2 text-right font-mono ${after < before ? 'text-[#a35b48]' : after > before ? 'text-[#3c7967]' : 'text-[#818885]'}`}>{after - before > 0 ? '+' : ''}{(after - before).toLocaleString()}</td></tr>;
                      })}</tbody></table>
                    </div>
                    <div className="mt-4 grid gap-4 md:grid-cols-2">
                      <div><div className="mb-2 font-mono text-[9px] uppercase tracking-[.12em] text-[#74807d]">Proposed operations</div>{preview.operations.length ? <ul className="space-y-1.5">{preview.operations.map((line, i) => <li key={`${line}-${i}`} className="flex gap-2 text-[10px] leading-4 text-[#53615e]"><Check size={12} className="mt-0.5 shrink-0 text-[#54816c]" />{line}</li>)}</ul> : <p className="text-[10px] text-[#858d88]">No operations proposed for these options.</p>}</div>
                      <div><div className="mb-2 font-mono text-[9px] uppercase tracking-[.12em] text-[#74807d]">Review notes</div>{preview.warnings.length ? <ul className="space-y-1.5">{preview.warnings.map((warning, i) => <li key={`${warning}-${i}`} className="flex gap-2 text-[10px] leading-4 text-[#8d6337]"><AlertCircle size={12} className="mt-0.5 shrink-0" />{warning}</li>)}</ul> : <p className="text-[10px] text-[#54806c]">No warnings returned.</p>}</div>
                    </div>
                    <div className="mt-5 flex flex-wrap items-center justify-between gap-3 border-t border-[#e8e2d5] pt-4">
                      <p className="flex items-center gap-2 text-[9px] text-[#66716d]"><ShieldCheck size={14} className="text-[#4f8069]" /> Apply only after review · FFT samples remain untouched</p>
                      <button onClick={applyProposal} disabled={applyCleanup.isPending || preview.fftSamplesChanged !== 0 || session.cleaned} className="inline-flex items-center justify-center gap-2 rounded-md bg-[#a56d31] px-4 py-2.5 text-[10px] font-bold text-[#fff9ed] hover:bg-[#8f5a26] disabled:cursor-not-allowed disabled:opacity-40" data-testid="button-apply-cleanup">
                        {applyCleanup.isPending ? <LoaderCircle size={13} className="animate-spin" /> : <CheckCircle2 size={13} />} {session.cleaned ? 'Cleanup already applied' : applyCleanup.isPending ? 'Applying…' : 'Apply reviewed changes'}
                      </button>
                    </div>
                    {applyCleanup.isError && <InlineError text={friendlyError(applyCleanup.error)} onRetry={applyProposal} />}
                  </div>}
                </div>

                <div className="flex flex-col gap-5">
                  <div className="overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5]">
                    <PanelHeading number="FFT" icon={<AudioLines size={15} />} title="Sample audit" tag={fft?.preserved ? 'Samples preserved' : 'Integrity check'} />
                    {fftQuery.isLoading ? <div className="space-y-3 p-5"><div className="skeleton h-5 w-2/3" /><div className="skeleton h-14" /></div> : fftQuery.isError ? <div className="px-5 py-4"><InlineError text={friendlyError(fftQuery.error)} onRetry={() => void fftQuery.refetch()} /></div> : fft ? <div className="p-5">
                      <div className="grid grid-cols-2 gap-3">
                        <div className="rounded-md bg-[#f1efe7] p-3"><div className="font-mono text-[9px] uppercase tracking-[.1em] text-[#858b85]">Captures</div><div className="mt-1.5 text-[20px] font-extrabold tracking-[-.05em]">{fft.captures.toLocaleString()}</div></div>
                        <div className="rounded-md bg-[#f1efe7] p-3"><div className="font-mono text-[9px] uppercase tracking-[.1em] text-[#858b85]">Samples</div><div className="mt-1.5 text-[20px] font-extrabold tracking-[-.05em]">{fft.samples.toLocaleString()}</div></div>
                      </div>
                      <div className={`mt-3 flex items-center gap-2 rounded-md px-3 py-2.5 text-[10px] font-semibold ${fft.preserved ? 'bg-[#e4eee5] text-[#467057]' : 'bg-[#f5e5df] text-[#9d5144]'}`} data-testid="status-fft-preserved"><ShieldCheck size={14} /> {fft.preserved ? 'Samples retained unchanged' : 'Preservation not confirmed'}</div>
                      {fft.findings.length > 0 ? <div className="mt-4 space-y-2">{fft.findings.map((finding) => <div key={finding.code} className="flex items-center justify-between gap-2 border-b border-[#ebe6dc] pb-2 last:border-0"><div className="min-w-0"><div className="truncate text-[10px] font-semibold text-[#586561]">{finding.label}</div><div className="mt-0.5 font-mono text-[8px] text-[#92958d]">{finding.code}</div></div><span className={`shrink-0 rounded px-2 py-1 font-mono text-[8px] uppercase ${finding.severity === 'critical' ? 'bg-[#f5e2de] text-[#9d4a41]' : finding.severity === 'warning' ? 'bg-[#f3ecd9] text-[#936c2d]' : 'bg-[#e7ece8] text-[#667c6d]'}`}>{finding.severity} · {finding.count}</span></div>)}</div> : <div className="mt-4 text-[10px] text-[#7b8580]">No findings reported for these samples.</div>}
                    </div> : null}
                  </div>
                  <ActivityPanel events={shownEvents} />
                </div>
              </section>

              <section className="animate-enter delay-3 mt-5 overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5]">
                <PanelHeading number="03" icon={<Activity size={15} />} title="Flight readings" tag={flightsQuery.isLoading ? 'Loading records' : `${(flights?.total ?? 0).toLocaleString()} matching records`} />
                <div className="flex flex-col gap-3 border-b border-[#e5e0d5] bg-[#f7f5ed] px-5 py-3.5 sm:flex-row sm:items-center sm:justify-between sm:px-6">
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <label className="relative"><Search size={13} className="absolute left-3 top-1/2 -translate-y-1/2 text-[#828b86]" /><input value={search} onChange={(event) => { setSearch(event.target.value); setFlightPage(1); }} placeholder="Search readings…" className="h-9 w-full rounded border border-[#dad5c9] bg-[#fcfbf6] pl-9 pr-3 text-[10px] outline-none placeholder:text-[#a1a39a] focus:border-[#5e8980] sm:w-[220px]" data-testid="input-flight-search" /></label>
                    <label className="relative"><Filter size={12} className="absolute left-3 top-1/2 -translate-y-1/2 text-[#828b86]" /><input value={tailFilter} onChange={(event) => { setTailFilter(event.target.value); setFlightPage(1); }} placeholder="Tail number" className="h-9 w-full rounded border border-[#dad5c9] bg-[#fcfbf6] pl-9 pr-3 text-[10px] outline-none placeholder:text-[#a1a39a] focus:border-[#5e8980] sm:w-[150px]" data-testid="input-tail-filter" /></label>
                    <label className="relative"><select value={conditionFilter} onChange={(event) => { setConditionFilter(event.target.value); setFlightPage(1); }} className="h-9 w-full appearance-none rounded border border-[#dad5c9] bg-[#fcfbf6] pl-3 pr-8 text-[10px] text-[#56625f] outline-none focus:border-[#5e8980] sm:w-[155px]" data-testid="select-flight-condition"><option value="">All conditions</option>{analytics?.byCondition.map((condition) => <option key={condition.name} value={condition.name}>{condition.name}</option>)}</select><ChevronRight size={12} className="pointer-events-none absolute right-2.5 top-1/2 -translate-y-1/2 rotate-90 text-[#828b86]" /></label>
                  </div>
                  <div className="font-mono text-[9px] text-[#838983]">FLIGHT READING INDEX</div>
                </div>
                {flightsQuery.isLoading ? <div className="space-y-2 p-5"><div className="skeleton h-8" /><div className="skeleton h-8" /><div className="skeleton h-8" /></div> : flightsQuery.isError ? <div className="p-5"><InlineError text={friendlyError(flightsQuery.error)} onRetry={() => void flightsQuery.refetch()} /></div> : flights?.items.length ? <>
                  <div className="overflow-x-auto"><table className="w-full min-w-[760px] text-left"><thead className="border-b border-[#e7e2d8] bg-[#f9f7f0] font-mono text-[8px] uppercase tracking-[.12em] text-[#7f8782]"><tr><th className="px-5 py-3">Tail / test</th><th className="px-4 py-3">Flight</th><th className="px-4 py-3">Date range</th><th className="px-4 py-3">Balance readings</th><th className="px-4 py-3">Track records</th><th className="px-4 py-3">Inspect</th></tr></thead>
                    {flights.items.map((flight, index) => {
                      const rowKey = `${flight.tailNumber}-${flight.testNumber}-${flight.flightNumber}-${index}`;
                      const expanded = expandedFlight === rowKey;
                      return <tbody key={rowKey} className="divide-y divide-[#eee9df]">
                        <tr className="hover:bg-[#f8f6ef]" data-testid={`row-flight-${index}`}>
                          <td className="px-5 py-3"><div className="font-mono text-[10px] font-medium text-[#33484a]">{flight.tailNumber || '—'}</div><div className="mt-1 font-mono text-[8px] text-[#92958e]">TEST {flight.testNumber || '—'}</div></td>
                          <td className="px-4 py-3 font-mono text-[10px] text-[#515f5c]">{flight.flightNumber || '—'}</td>
                          <td className="px-4 py-3 text-[9px] text-[#707b76]">{formatDate(flight.started)} <span className="text-[#a3a59a]">→</span> {formatDate(flight.ended)}</td>
                          <td className="px-4 py-3"><div className="font-mono text-[10px] text-[#455b57]">{flight.balanceData.length}</div><div className="mt-1 max-w-[220px] truncate text-[8px] text-[#92978f]">{flight.balanceData.slice(0, 2).map((item) => `${item.part} · ${item.axis}`).join(' / ') || 'No balance data'}</div></td>
                          <td className="px-4 py-3 font-mono text-[10px] text-[#455b57]">{flight.trackData.length}</td>
                          <td className="px-4 py-3"><button onClick={() => setExpandedFlight(expanded ? '' : rowKey)} className="inline-flex items-center gap-1 rounded px-2 py-1 text-[9px] font-semibold text-[#367169] hover:bg-[#e8eee8]" aria-expanded={expanded} data-testid={`button-inspect-flight-${index}`}>{expanded ? 'Hide' : 'Details'} <ChevronRight size={11} className={expanded ? 'rotate-90' : ''} /></button></td>
                        </tr>
                        {expanded && <tr><td colSpan={6} className="bg-[#f5f4ec] px-5 py-4 sm:px-7">
                          <div className="grid gap-4 md:grid-cols-2">
                            <div><div className="mb-2 font-mono text-[8px] uppercase tracking-[.12em] text-[#78827c]">Balance observations</div>{flight.balanceData.length ? <div className="space-y-1.5">{flight.balanceData.map((reading, readingIndex) => <div key={`${reading.part}-${reading.axis}-${readingIndex}`} className="flex flex-wrap items-center justify-between gap-2 rounded border border-[#e4e0d4] bg-[#fbfaf5] px-3 py-2 text-[9px]"><span className="font-semibold text-[#4c5c58]">{reading.testCondition} · {reading.part} · {reading.axis}</span><span className="font-mono text-[#5f706b]">Amp {reading.amplitude ?? '—'} / Phase {reading.phase ?? '—'}</span></div>)}</div> : <div className="text-[9px] text-[#8b918a]">No balance observations.</div>}</div>
                            <div><div className="mb-2 font-mono text-[8px] uppercase tracking-[.12em] text-[#78827c]">Track record samples</div>{flight.trackData.length ? <pre className="max-h-48 overflow-auto rounded border border-[#e4e0d4] bg-[#fbfaf5] p-3 font-mono text-[8px] leading-4 text-[#596760]">{JSON.stringify(flight.trackData, null, 2)}</pre> : <div className="text-[9px] text-[#8b918a]">No track records.</div>}</div>
                          </div>
                        </td></tr>}
                      </tbody>;
                    })}
                  </table></div>
                  <div className="flex items-center justify-between border-t border-[#e5e0d5] px-5 py-3 sm:px-6"><span className="font-mono text-[9px] text-[#828983]">PAGE {flights.page} · {flights.total.toLocaleString()} TOTAL</span><div className="flex gap-1"><button onClick={() => setFlightPage((page) => Math.max(1, page - 1))} disabled={flightPage <= 1} className="rounded border border-[#dcd7cb] p-1.5 text-[#65716e] hover:bg-[#f0eee6] disabled:opacity-35" aria-label="Previous page" data-testid="button-flights-previous"><ChevronLeft size={14} /></button><button onClick={() => setFlightPage((page) => page + 1)} disabled={flightPage * (flights.limit || 6) >= flights.total} className="rounded border border-[#dcd7cb] p-1.5 text-[#65716e] hover:bg-[#f0eee6] disabled:opacity-35" aria-label="Next page" data-testid="button-flights-next"><ChevronRight size={14} /></button></div></div>
                </> : <div className="px-6 py-12 text-center" data-testid="empty-flight-results"><div className="mx-auto flex h-10 w-10 items-center justify-center rounded-full bg-[#efede4] text-[#71807b]"><Search size={17} /></div><div className="mt-3 text-[11px] font-bold text-[#53625e]">No readings match this view</div><p className="mt-1 text-[9px] text-[#8a918b]">Adjust the search or tail filter to inspect another flight.</p></div>}
              </section>

              <section className="mt-5 grid gap-5 xl:grid-cols-[minmax(0,1fr)_minmax(310px,.72fr)]">
                <div className="overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5]">
                  <PanelHeading number="04" icon={<ArrowDownToLine size={15} />} title="Export cleaned dataset" tag={session.cleaned ? 'Ready to stream' : 'Available after apply'} />
                  <div className="flex flex-col gap-4 px-5 py-5 sm:flex-row sm:items-center sm:justify-between sm:px-6">
                    <div className="flex items-start gap-3"><div className="mt-0.5 flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-[#e8e6dc] text-[#526d68]"><HardDrive size={17} /></div><div><div className="text-[11px] font-bold text-[#3f514f]">Original schema, cleaned copy</div><p className="mt-1 max-w-[400px] text-[9px] leading-4 text-[#858b85]">The export link streams the server-prepared file directly to your browser. No client-side reconstruction.</p></div></div>
                    {exportQuery.data?.downloadUrl ? <a href={exportQuery.data.downloadUrl} download={exportQuery.data.filename} className="inline-flex shrink-0 items-center justify-center gap-2 rounded-md bg-[#1f5960] px-4 py-2.5 text-[10px] font-bold text-white hover:bg-[#17484f]" data-testid="link-download-export"><ArrowDownToLine size={14} /> Download {exportQuery.data.filename}</a> : <button onClick={beginExport} disabled={!session.cleaned || exportQuery.isFetching} className="inline-flex shrink-0 items-center justify-center gap-2 rounded-md bg-[#1f5960] px-4 py-2.5 text-[10px] font-bold text-white hover:bg-[#17484f] disabled:cursor-not-allowed disabled:opacity-40" data-testid="button-prepare-export">{exportQuery.isFetching ? <LoaderCircle size={13} className="animate-spin" /> : <ArrowDownToLine size={14} />}{exportQuery.isFetching ? 'Preparing link…' : session.cleaned ? 'Prepare download' : 'Apply cleanup first'}</button>}
                  </div>
                  {exportQuery.isError && <div className="px-5 pb-4"><InlineError text={friendlyError(exportQuery.error)} onRetry={beginExport} /></div>}
                  {exportQuery.data && <div className="flex flex-wrap items-center gap-2 border-t border-[#e7e2d7] bg-[#f7f5ed] px-5 py-2.5 font-mono text-[9px] text-[#71807b]" data-testid="status-export-ready"><CheckCircle2 size={12} className="text-[#4d8068]" /> {exportQuery.data.filename} · {prettyBytes(exportQuery.data.bytes)} · browser download</div>}
                </div>
                <ActivityPanel events={shownEvents} />
              </section>
              <div className="mt-5 flex items-center justify-center gap-2 font-mono text-[8px] uppercase tracking-[.1em] text-[#9a9b90]"><History size={12} /> Temporary session · source snapshot retained until expiry or close</div>
            </>
          )}
        </div>
      </main>
    </div>
  );
}

function CountCell({ label, value, compact = false }: { label: string; value: number; compact?: boolean }) {
  return <div className={`bg-[#f9f7f0] ${compact ? 'px-3 py-4' : 'px-4 py-4 sm:px-5'}`} data-testid={`metric-${label.toLowerCase().replaceAll(' ', '-')}`}>
    <div className="font-mono text-[8px] uppercase tracking-[.09em] text-[#858b83]">{label}</div>
    <div className={`mt-1.5 font-extrabold tracking-[-.05em] text-[#2d4145] ${compact ? 'text-[17px]' : 'text-[20px]'}`}>{value.toLocaleString()}</div>
  </div>;
}

function PanelHeading({ number, icon, title, tag }: { number: string; icon: ReactNode; title: string; tag?: string }) {
  return <div className="flex flex-wrap items-center justify-between gap-2 border-b border-[#e3ded3] px-5 py-4 sm:px-6">
    <div className="flex items-center gap-3"><span className="flex h-8 w-8 items-center justify-center rounded-md bg-[#e8ece5] text-[#44746a]">{icon}</span><div><div className="text-[12px] font-bold text-[#344649]">{title}</div><div className="mt-0.5 font-mono text-[8px] uppercase tracking-[.12em] text-[#91958c]">Section {number}</div></div></div>
    {tag && <span className="rounded bg-[#f0eee6] px-2 py-1 font-mono text-[8px] uppercase tracking-[.08em] text-[#79827c]">{tag}</span>}
  </div>;
}

function ActivityPanel({ events }: { events: LocalEvent[] }) {
  return <div className="overflow-hidden rounded-xl border border-[#d8d2c5] bg-[#fbfaf5]">
    <PanelHeading number="LOG" icon={<History size={15} />} title="Session activity" tag="Audit trail" />
    <div className="px-5 py-3.5 sm:px-6">
      {events.length ? <div className="space-y-3">{events.map((event, index) => <div key={`${event.label}-${event.time}-${index}`} className="flex items-start gap-2.5" data-testid={`activity-event-${index}`}>
        <span className={`mt-1.5 h-1.5 w-1.5 shrink-0 rounded-full ${event.tone === 'warning' ? 'bg-[#b16b4e]' : event.tone === 'success' ? 'bg-[#528068]' : 'bg-[#b28a42]'}`} />
        <div className="min-w-0 flex-1 text-[9px] leading-4 text-[#65716d]">{event.label}</div><time className="shrink-0 font-mono text-[8px] text-[#a0a195]">{event.time}</time>
      </div>)}</div> : <div className="flex items-center gap-2 py-1 text-[10px] text-[#8d928a]" data-testid="empty-activity"><Activity size={13} /> No actions recorded yet.</div>}
    </div>
  </div>;
}

function InlineError({ text, onRetry }: { text: string; onRetry: () => void }) {
  return <div className="mt-3 flex flex-wrap items-center justify-between gap-2 rounded-md border border-[#e5c9be] bg-[#fbefeb] px-3 py-2.5" role="alert" data-testid="status-error">
    <div className="flex min-w-0 items-center gap-2 text-[10px] text-[#914e41]"><AlertCircle size={13} className="shrink-0" /><span className="truncate">{text}</span></div>
    <button onClick={onRetry} className="inline-flex items-center gap-1.5 text-[9px] font-bold text-[#804236] hover:underline" data-testid="button-retry"><RefreshCw size={11} /> Retry</button>
  </div>;
}

function formatDuration(seconds: number) {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  return hours ? `${hours}h ${minutes}m` : `${minutes}m`;
}
function formatDate(value: string | null) {
  if (!value) return '—';
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString(undefined, { year: 'numeric', month: 'short', day: '2-digit' });
}

export default App;
