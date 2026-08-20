import { useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { AlertCircle, Bot, Loader2 } from "lucide-react";
import { api } from "@/lib/api";
import { auth } from "@/lib/auth";

/**
 * Sign-in for AUTH_MODE=local.
 *
 * Shown by the shell whenever authentication is enabled and no valid token is
 * held. Roles matter here: an operator can approve service-level actions, but
 * a region failover needs an approver — so the screen says so rather than
 * letting someone discover it at the moment they are trying to act.
 */
const Login = ({ mode }: { mode: string }) => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    setBusy(true);
    setError(null);
    try {
      const res = await api.login(username, password);
      auth.set(res.token, res.principal);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="flex min-h-screen items-center justify-center bg-background p-4">
      <Card className="w-full max-w-sm">
        <CardHeader className="pb-4">
          <CardTitle className="flex items-center gap-2 text-lg">
            <div className="rounded-lg bg-primary/15 p-1.5">
              <Bot className="h-5 w-5 text-primary" />
            </div>
            Kafka Guardian
          </CardTitle>
          <p className="text-xs text-muted-foreground">
            {mode === "oidc"
              ? "This deployment uses single sign-on. Obtain a token from your identity provider."
              : "Sign in to approve actions and change configuration."}
          </p>
        </CardHeader>

        <CardContent>
          {mode === "oidc" ? (
            <p className="text-sm text-muted-foreground">
              Password sign-in is disabled here.
            </p>
          ) : (
            <form onSubmit={submit} className="space-y-3">
              <div className="space-y-1.5">
                <Label htmlFor="u" className="text-xs">Username</Label>
                <Input id="u" value={username} autoFocus autoComplete="username"
                       onChange={(e) => setUsername(e.target.value)} />
              </div>
              <div className="space-y-1.5">
                <Label htmlFor="p" className="text-xs">Password</Label>
                <Input id="p" type="password" value={password}
                       autoComplete="current-password"
                       onChange={(e) => setPassword(e.target.value)} />
              </div>

              {error && (
                <div className="flex items-start gap-1.5 rounded-lg bg-destructive/10 px-2.5 py-2 text-xs text-destructive">
                  <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                  {error}
                </div>
              )}

              <Button type="submit" className="w-full"
                      disabled={busy || !username || !password}>
                {busy && <Loader2 className="mr-1.5 h-4 w-4 animate-spin" />}
                Sign in
              </Button>
            </form>
          )}

          <p className="mt-4 border-t border-border/50 pt-3 text-[11px] leading-relaxed text-muted-foreground">
            What you can approve depends on your role. An operator can approve
            actions affecting one service; a region failover needs an approver.
          </p>
        </CardContent>
      </Card>
    </div>
  );
};

export default Login;
