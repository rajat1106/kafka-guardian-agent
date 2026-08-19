import { useCallback, useEffect, useMemo, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import {
  Select, SelectContent, SelectItem, SelectTrigger, SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import {
  AlertCircle, Boxes, Brain, Bell, CheckCircle2, Container, Gauge, Loader2,
  Play, Plug, Square, Zap,
} from "lucide-react";
import {
  api, type PluginState, type PluginsResponse, type SlotSpec, type ProviderSpec,
} from "@/lib/api";

const SLOT_ICON: Record<string, typeof Plug> = {
  source: Boxes, brain: Brain, notify: Bell, monitor: Gauge,
};

const STATUS_STYLE: Record<string, string> = {
  connected: "border-emerald-400/40 bg-emerald-500/10 text-emerald-400",
  error: "border-rose-400/40 bg-rose-500/10 text-rose-400",
  unconfigured: "border-border bg-muted/40 text-muted-foreground",
  testing: "border-sky-400/40 bg-sky-500/10 text-sky-400",
};

/* ── one slot ────────────────────────────────────────────────────── */

interface SlotProps {
  spec: SlotSpec;
  current: PluginState | undefined;
  onSaved: (r: PluginsResponse) => void;
}

const SlotCard = ({ spec, current, onSaved }: SlotProps) => {
  const Icon = SLOT_ICON[spec.slot] ?? Plug;
  const [providerId, setProviderId] = useState(current?.provider_id ?? spec.default);
  const [values, setValues] = useState<Record<string, string>>({});
  const [busy, setBusy] = useState<"idle" | "saving" | "testing">("idle");
  const [result, setResult] = useState<Record<string, unknown> | null>(null);

  const chosen: ProviderSpec | undefined = useMemo(
    () => spec.providers.find((p) => p.id === providerId),
    [spec.providers, providerId],
  );

  // When the slot already holds this provider, seed the form with the masked
  // config so the user can see what is set without it being sent back.
  useEffect(() => {
    if (current?.provider_id === providerId) {
      setValues(
        Object.fromEntries(
          Object.entries(current.config ?? {}).map(([k, v]) => [k, String(v ?? "")]),
        ),
      );
    } else {
      setValues(
        Object.fromEntries(
          (chosen?.fields ?? [])
            .filter((f) => f.default !== null && f.default !== undefined)
            .map((f) => [f.name, String(f.default)]),
        ),
      );
    }
    setResult(null);
  }, [providerId, current?.provider_id]); // eslint-disable-line react-hooks/exhaustive-deps

  const dirty = providerId !== current?.provider_id;
  const status = dirty ? "unconfigured" : (current?.status ?? "unconfigured");
  const lastTest = dirty ? null : current?.last_test;

  const missing = (chosen?.fields ?? []).filter(
    (f) => f.required && !(values[f.name] ?? "").trim(),
  );

  const save = useCallback(async () => {
    setBusy("saving");
    try {
      const r = await api.configurePlugin(spec.slot, providerId, values);
      onSaved(r as unknown as PluginsResponse);
      setResult(null);
    } finally {
      setBusy("idle");
    }
  }, [spec.slot, providerId, values, onSaved]);

  const saveAndTest = useCallback(async () => {
    setBusy("saving");
    try {
      await api.configurePlugin(spec.slot, providerId, values);
      setBusy("testing");
      const r = await api.testPlugin(spec.slot);
      setResult(r);
      onSaved(r as unknown as PluginsResponse);
    } catch (e) {
      setResult({ ok: false, error: String(e) });
    } finally {
      setBusy("idle");
    }
  }, [spec.slot, providerId, values, onSaved]);

  return (
    <Card className={cn(
      "transition-colors",
      status === "connected" && !dirty && "border-emerald-400/30",
      status === "error" && "border-rose-400/40",
    )}>
      <CardHeader className="pb-3">
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          <Icon className="h-5 w-5 text-primary" />
          {spec.title}
          {!spec.required && (
            <Badge variant="outline" className="text-[10px]">optional</Badge>
          )}
          <Badge variant="outline"
                 className={cn("ml-auto text-[10px] capitalize", STATUS_STYLE[status])}>
            {status === "connected" ? "connected" : status}
          </Badge>
        </CardTitle>
        <p className="text-xs text-muted-foreground">{spec.description}</p>
      </CardHeader>

      <CardContent className="space-y-3">
        <div className="space-y-1.5">
          <Label className="text-xs">Provider</Label>
          <Select value={providerId} onValueChange={setProviderId}>
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              {spec.providers.map((p) => (
                <SelectItem key={p.id} value={p.id}>
                  <span className="flex items-center gap-2">
                    {p.name}
                    {p.recommended && (
                      <span className="text-[10px] text-primary">recommended</span>
                    )}
                  </span>
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {chosen && (
            <p className="text-xs text-muted-foreground">{chosen.summary}</p>
          )}
        </div>

        {chosen?.notes && (
          <p className="rounded-lg bg-muted/40 px-3 py-2 text-xs leading-relaxed text-muted-foreground">
            {chosen.notes}
          </p>
        )}

        {(chosen?.fields ?? []).map((f) => (
          <div key={f.name} className="space-y-1">
            <Label className="text-xs">
              {f.label}
              {!f.required && (
                <span className="ml-1 text-muted-foreground">(optional)</span>
              )}
            </Label>
            {f.type === "select" ? (
              <Select
                value={values[f.name] ?? String(f.default ?? "")}
                onValueChange={(v) => setValues((s) => ({ ...s, [f.name]: v }))}
              >
                <SelectTrigger><SelectValue /></SelectTrigger>
                <SelectContent>
                  {f.options.map((o) => (
                    <SelectItem key={o.value} value={o.value}>{o.label}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : f.type === "textarea" ? (
              <Textarea
                rows={3}
                placeholder={f.placeholder}
                value={values[f.name] ?? ""}
                onChange={(e) => setValues((s) => ({ ...s, [f.name]: e.target.value }))}
              />
            ) : (
              <Input
                type={f.type === "password" ? "password" : f.type === "number" ? "number" : "text"}
                placeholder={f.placeholder}
                value={values[f.name] ?? ""}
                onChange={(e) => setValues((s) => ({ ...s, [f.name]: e.target.value }))}
                autoComplete={f.secret ? "new-password" : "off"}
              />
            )}
            {f.help && <p className="text-[11px] text-muted-foreground">{f.help}</p>}
            {f.secret && (values[f.name] ?? "").startsWith("••••") && (
              <p className="text-[11px] text-emerald-400/80">
                Already saved. Leave as-is to keep it, or type a new value to replace it.
              </p>
            )}
          </div>
        ))}

        {/* test result */}
        {(result || lastTest) && (
          <div className={cn(
            "flex items-start gap-2 rounded-lg px-3 py-2 text-xs",
            (result?.ok ?? lastTest?.ok)
              ? "bg-emerald-500/10 text-emerald-300"
              : "bg-rose-500/10 text-rose-300",
          )}>
            {(result?.ok ?? lastTest?.ok)
              ? <CheckCircle2 className="mt-0.5 h-3.5 w-3.5 shrink-0" />
              : <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />}
            <div className="min-w-0">
              <div>
                {String(
                  (result?.summary ?? result?.error ??
                   lastTest?.summary ?? lastTest?.error ?? "") || "Tested.",
                )}
              </div>
              {lastTest?.at && !result && (
                <div className="mt-0.5 opacity-70">
                  last tested {new Date(String(lastTest.at)).toLocaleString()}
                </div>
              )}
            </div>
          </div>
        )}

        <div className="flex flex-wrap gap-2 pt-1">
          {chosen?.zero_config ? (
            <Button size="sm" onClick={save} disabled={busy !== "idle" || !dirty}>
              {busy === "saving" && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
              {dirty ? "Use this" : "In use"}
            </Button>
          ) : (
            <>
              <Button
                size="sm"
                onClick={saveAndTest}
                disabled={busy !== "idle" || missing.length > 0}
                title={missing.length
                  ? `Fill in: ${missing.map((f) => f.label).join(", ")}`
                  : undefined}
              >
                {busy === "testing"
                  ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                  : <Zap className="mr-1.5 h-4 w-4" />}
                {busy === "testing" ? "Testing…" : "Save and test connection"}
              </Button>
              {missing.length > 0 && (
                <span className="self-center text-xs text-muted-foreground">
                  Needs {missing.map((f) => f.label).join(", ")}
                </span>
              )}
            </>
          )}
        </div>

        {!chosen?.zero_config && spec.slot === "brain" && (
          <p className="text-[11px] text-muted-foreground">
            The agent will not use a model until the test passes — an untested
            key is one that has never been proven, and an incident is the wrong
            time to find out.
          </p>
        )}
      </CardContent>
    </Card>
  );
};

/* ── demo cluster controls ───────────────────────────────────────── */

const DemoPanel = ({ demo, refresh }: { demo: PluginsResponse["demo"]; refresh: () => void }) => {
  const [busy, setBusy] = useState<string | null>(null);
  const [scenarios, setScenarios] = useState<{
    simulated: { key: string; title: string; service: string; description: string }[];
    infrastructure: { key: string; title: string; description: string; danger: string }[];
  } | null>(null);

  useEffect(() => { api.demoScenarios().then(setScenarios).catch(() => undefined); }, []);

  const run = async (label: string, fn: () => Promise<unknown>) => {
    setBusy(label);
    try { await fn(); refresh(); } finally { setBusy(null); }
  };

  if (!demo?.available) {
    return (
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex items-center gap-2 text-base">
            <Container className="h-5 w-5 text-muted-foreground" />
            Demo cluster
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">
            Docker is not reachable from the API container, so the demo cluster
            cannot be controlled from here. {demo?.error}
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader className="pb-3">
        <CardTitle className="flex flex-wrap items-center gap-2 text-base">
          <Container className="h-5 w-5 text-primary" />
          Demo cluster
          <Badge variant="outline" className={cn("ml-auto text-[10px]",
            demo.all_up ? "border-emerald-400/40 text-emerald-400"
                        : "border-amber-400/40 text-amber-400")}>
            {demo.running}/{demo.total} containers running
          </Badge>
        </CardTitle>
        <p className="text-xs text-muted-foreground">
          A real Apache Kafka broker in Docker with simulated services. Starting
          it here starts the actual containers.
        </p>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-4">
          {demo.containers.map((c) => (
            <div key={c.name} className="rounded-lg bg-muted/40 px-2.5 py-2">
              <div className="flex items-center gap-1.5">
                <span className={cn("h-1.5 w-1.5 rounded-full",
                  c.running ? "bg-emerald-400" : "bg-muted-foreground/50")} />
                <span className="truncate text-xs font-medium">{c.service}</span>
              </div>
              <div className="mt-0.5 font-mono text-[10px] text-muted-foreground">
                {c.status}{c.health ? ` · ${c.health}` : ""}
              </div>
            </div>
          ))}
        </div>

        <div className="flex flex-wrap gap-2">
          <Button size="sm" variant="outline" disabled={!!busy}
                  onClick={() => run("start", api.demoStart)}>
            {busy === "start" ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                              : <Play className="mr-1.5 h-4 w-4" />}
            Start cluster
          </Button>
          <Button size="sm" variant="outline" disabled={!!busy}
                  onClick={() => run("stop", api.demoStop)}>
            {busy === "stop" ? <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />
                             : <Square className="mr-1.5 h-4 w-4" />}
            Stop cluster
          </Button>
        </div>

        {scenarios && (
          <div className="space-y-3">
            <div>
              <div className="mb-1.5 text-xs font-medium text-muted-foreground">
                Break a service — simulated faults inside the running services
              </div>
              <div className="grid gap-1.5 sm:grid-cols-2">
                {scenarios.simulated.map((s) => (
                  <button
                    key={s.key}
                    disabled={!!busy}
                    onClick={() => run(s.key, () => api.inject(s.key))}
                    className="rounded-lg border border-border/50 px-2.5 py-2 text-left transition-colors hover:border-primary/50 hover:bg-muted/40 disabled:opacity-50"
                  >
                    <div className="text-xs font-medium">{s.title}</div>
                    <div className="text-[10px] text-muted-foreground">{s.service}</div>
                  </button>
                ))}
              </div>
            </div>

            <div>
              <div className="mb-1.5 text-xs font-medium text-muted-foreground">
                Break the infrastructure — acts on the Docker containers themselves
              </div>
              <div className="grid gap-1.5 sm:grid-cols-2">
                {scenarios.infrastructure.map((s) => (
                  <div key={s.key}
                       className="rounded-lg border border-rose-400/25 bg-rose-500/5 px-2.5 py-2">
                    <div className="flex items-center gap-1.5">
                      <span className="text-xs font-medium">{s.title}</span>
                      <Badge variant="outline"
                             className="ml-auto border-rose-400/40 text-[9px] text-rose-300">
                        {s.danger}
                      </Badge>
                    </div>
                    <p className="mt-0.5 text-[10px] leading-relaxed text-muted-foreground">
                      {s.description}
                    </p>
                    <div className="mt-1.5 flex gap-1.5">
                      <Button size="sm" variant="outline" className="h-6 px-2 text-[10px]"
                              disabled={!!busy}
                              onClick={() => run(s.key, () => api.injectInfra(s.key))}>
                        Break it
                      </Button>
                      <Button size="sm" variant="ghost" className="h-6 px-2 text-[10px]"
                              disabled={!!busy}
                              onClick={() => run(`${s.key}-r`, () => api.recoverInfra(s.key))}>
                        Recover
                      </Button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
};

/* ── page ────────────────────────────────────────────────────────── */

const Connections = () => {
  const [data, setData] = useState<PluginsResponse | null>(null);
  const [discovered, setDiscovered] = useState<Record<string, unknown> | null>(null);
  const [discovering, setDiscovering] = useState(false);

  const load = useCallback(() => {
    api.plugins().then(setData).catch(() => undefined);
  }, []);
  useEffect(() => { load(); const t = setInterval(load, 8000); return () => clearInterval(t); },
            [load]);

  const byslot = useMemo(
    () => Object.fromEntries((data?.configured ?? []).map((c) => [c.slot, c])),
    [data],
  );

  const discover = async () => {
    setDiscovering(true);
    try { setDiscovered(await api.discoverSource()); }
    finally { setDiscovering(false); }
  };

  const source = byslot["source"];

  return (
    <div className="mx-auto max-w-[1600px] space-y-5 px-4 py-5 md:px-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Connections</h1>
        <p className="mt-0.5 text-sm text-muted-foreground">
          Plug in the cluster you want watched, the engine that makes decisions,
          and where you want to hear about it. Credentials are stored on the
          server and never sent back to this page.
        </p>
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        {(data?.catalogue ?? []).map((spec) => (
          <SlotCard key={spec.slot} spec={spec} current={byslot[spec.slot]}
                    onSaved={(r) => { if (r?.plugins) { load(); } else { load(); } }} />
        ))}
      </div>

      {/* what's actually on the connected cluster */}
      <Card>
        <CardHeader className="pb-3">
          <CardTitle className="flex flex-wrap items-center gap-2 text-base">
            <Boxes className="h-5 w-5 text-primary" />
            What is on the cluster
            <Button size="sm" variant="outline" className="ml-auto"
                    disabled={discovering || source?.status !== "connected"}
                    onClick={discover}>
              {discovering && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
              Fetch topics
            </Button>
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            {source?.status === "connected"
              ? "Reads the live cluster: every topic, its partitions and replication factor."
              : "Connect a Kafka cluster above first."}
          </p>
        </CardHeader>
        {discovered && (
          <CardContent>
            {discovered.ok === false ? (
              <p className="text-sm text-rose-400">{String(discovered.error)}</p>
            ) : (
              <>
                <div className="mb-2 flex flex-wrap gap-4 text-xs text-muted-foreground">
                  <span>{(discovered.topics as unknown[])?.length ?? 0} topics</span>
                  <span>{String(discovered.total_partitions ?? 0)} partitions</span>
                  <span>
                    {(discovered.consumer_groups as unknown[])?.length ?? 0} consumer groups
                  </span>
                </div>
                <div className="max-h-72 space-y-1 overflow-y-auto pr-2">
                  {(discovered.topics as {
                    name: string; partitions: number; replication_factor: number;
                    internal: boolean; owned_by_guardian: boolean;
                  }[]).map((t) => (
                    <div key={t.name}
                         className="flex items-center gap-2 rounded bg-muted/30 px-2.5 py-1.5">
                      <span className="truncate font-mono text-xs">{t.name}</span>
                      {t.owned_by_guardian && (
                        <Badge variant="outline" className="text-[9px]">guardian</Badge>
                      )}
                      {t.internal && (
                        <Badge variant="outline" className="text-[9px]">internal</Badge>
                      )}
                      <span className="ml-auto shrink-0 font-mono text-[10px] text-muted-foreground">
                        {t.partitions}p · rf{t.replication_factor}
                      </span>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        )}
      </Card>

      <DemoPanel demo={data?.demo} refresh={load} />
    </div>
  );
};

export default Connections;
