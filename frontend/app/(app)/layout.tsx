import { Navbar } from "@/components/navbar";
import { getSession } from "@/lib/session";
import { redirect } from "next/navigation";

export default async function AppLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const session = await getSession();
  if (!session) redirect("/login");

  return (
    <div className="relative min-h-screen">
      <Navbar />
      <main className="relative z-0">{children}</main>
    </div>
  );
}
