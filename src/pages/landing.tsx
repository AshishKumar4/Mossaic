import { Link } from "react-router-dom";
import { motion } from "framer-motion";
import {
  HardDrive,
  ArrowRight,
  Shield,
  Zap,
  Globe,
  Database,
  Layers,
  Lock,
  Upload,
  Download,
  CheckCircle2,
} from "lucide-react";
import { Button } from "@/components/ui/button";

const features = [
  {
    icon: Zap,
    title: "Parallel Uploads",
    description:
      "Files split into 1 MB chunks uploaded across 6 concurrent streams for maximum throughput.",
  },
  {
    icon: Shield,
    title: "Content-Addressed Storage",
    description:
      "Every chunk identified by SHA-256 hash. Automatic deduplication saves storage and bandwidth.",
  },
  {
    icon: Globe,
    title: "Edge-First Architecture",
    description:
      "Built on Cloudflare Workers and Durable Objects. Your data served from 300+ locations worldwide.",
  },
  {
    icon: Database,
    title: "Durable Object Sharding",
    description:
      "Rendezvous hashing distributes chunks across unlimited ShardDOs for infinite horizontal scaling.",
  },
  {
    icon: Lock,
    title: "Secure by Default",
    description:
      "JWT authentication, bcrypt passwords, and isolated per-user storage with configurable quotas.",
  },
  {
    icon: Layers,
    title: "Zero-Buffer Streaming",
    description:
      "Worker streams chunk data directly -- never buffers entire files in memory. Handles any file size.",
  },
];

const steps = [
  {
    icon: Upload,
    title: "Upload",
    description:
      "Drop files into Mossaic. They're split into chunks, hashed, and uploaded in parallel.",
  },
  {
    icon: Database,
    title: "Distribute",
    description:
      "Chunks are placed across Durable Object shards using rendezvous hashing. Duplicates are eliminated.",
  },
  {
    icon: Download,
    title: "Retrieve",
    description:
      "Download files with parallel chunk fetching and streaming reassembly. Fast from any edge location.",
  },
];

const container = {
  hidden: { opacity: 0 },
  show: {
    opacity: 1,
    transition: { staggerChildren: 0.08 },
  },
};

const item = {
  hidden: { opacity: 0, y: 20 },
  show: { opacity: 1, y: 0, transition: { duration: 0.4, ease: "easeOut" as const } },
};

export function LandingPage() {
  return (
    <div className="min-h-screen bg-background">
      {/* Nav */}
      <nav className="sticky top-0 z-40 border-b border-white/[0.06] bg-background/80 backdrop-blur-xl">
        <div className="mx-auto flex h-14 max-w-6xl items-center justify-between px-6">
          <Link to="/" className="flex items-center gap-2.5">
            <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary">
              <HardDrive className="h-4 w-4 text-primary-foreground" />
            </div>
            <span className="text-base font-bold tracking-tight text-heading">
              Mossaic
            </span>
          </Link>
          <div className="flex items-center gap-3">
            <Button variant="ghost" size="sm" asChild>
              <Link to="/login">Sign in</Link>
            </Button>
            <Button size="sm" asChild>
              <Link to="/login">
                Get Started
                <ArrowRight className="ml-1 h-3.5 w-3.5" />
              </Link>
            </Button>
          </div>
        </div>
      </nav>

      {/* Hero */}
      <section className="relative overflow-hidden">
        {/* Background effects — subtle warm glow */}
        <div className="pointer-events-none absolute inset-0">
          <div className="absolute left-1/2 top-0 h-[600px] w-[800px] -translate-x-1/2 -translate-y-1/2 rounded-full bg-primary/[0.04] blur-3xl" />
          <div className="absolute bottom-0 right-0 h-[400px] w-[600px] translate-x-1/4 translate-y-1/4 rounded-full bg-primary/[0.03] blur-3xl" />
        </div>

        <div className="relative mx-auto max-w-4xl px-6 pb-24 pt-20 text-center sm:pt-28 lg:pt-36">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.6, ease: "easeOut" }}
          >
            <div className="mb-6 inline-flex items-center gap-2 rounded-full border border-white/[0.06] bg-white/[0.03] px-4 py-1.5 text-xs font-medium text-muted-foreground backdrop-blur-sm">
              <div className="h-1.5 w-1.5 rounded-full bg-success animate-pulse" />
              Built on Cloudflare Workers + Durable Objects
            </div>

            <h1 className="text-4xl font-bold tracking-tight text-heading sm:text-5xl lg:text-6xl">
              Distributed file storage,{" "}
              <span className="text-primary">
                infinitely scalable
              </span>
            </h1>

            <p className="mx-auto mt-6 max-w-2xl text-base text-muted-foreground leading-relaxed sm:text-lg">
              Parallel chunked uploads with content-addressed deduplication across
              Cloudflare's global edge. Zero-buffer streaming. No file size limits.
            </p>

            <div className="mt-10 flex flex-col items-center justify-center gap-3 sm:flex-row">
              <Button size="lg" className="rounded-lg px-8" asChild>
                <Link to="/login">
                  Start Uploading
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Link>
              </Button>
              <Button variant="outline" size="lg" className="rounded-lg px-8" asChild>
                <a
                  href="https://github.com"
                  target="_blank"
                  rel="noopener noreferrer"
                >
                  View on GitHub
                </a>
              </Button>
            </div>
          </motion.div>

          {/* Stats bar */}
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.3 }}
            className="mx-auto mt-16 flex max-w-lg flex-wrap items-center justify-center gap-8 sm:gap-12"
          >
            {[
              { value: "1 MB", label: "Chunk size" },
              { value: "SHA-256", label: "Content hash" },
              { value: "\u221E", label: "No file limit" },
              { value: "300+", label: "Edge locations" },
            ].map((stat) => (
              <div key={stat.label} className="text-center">
                <div className="text-xl font-bold tracking-tight text-heading sm:text-2xl">
                  {stat.value}
                </div>
                <div className="mt-0.5 text-xs text-muted-foreground">
                  {stat.label}
                </div>
              </div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* Features */}
      <section className="border-t border-white/[0.06] bg-white/[0.02]">
        <div className="mx-auto max-w-6xl px-6 py-24">
          <div className="text-center">
            <h2 className="text-2xl font-bold tracking-tight text-heading sm:text-3xl">
              Everything you need for file storage at scale
            </h2>
            <p className="mx-auto mt-3 max-w-xl text-sm text-muted-foreground sm:text-base">
              Built from the ground up for performance, reliability, and
              developer experience.
            </p>
          </div>

          <motion.div
            variants={container}
            initial="hidden"
            whileInView="show"
            viewport={{ once: true, margin: "-100px" }}
            className="mt-16 grid gap-4 sm:grid-cols-2 lg:grid-cols-3"
          >
            {features.map((feature) => (
              <motion.div
                key={feature.title}
                variants={item}
                className="group rounded-xl border border-white/[0.06] bg-white/[0.03] p-6 transition-all duration-200 hover:border-border hover:bg-white/[0.05]"
              >
                <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-primary/10 transition-colors duration-200 group-hover:bg-primary/15">
                  <feature.icon className="h-5 w-5 text-primary" />
                </div>
                <h3 className="mt-4 text-sm font-semibold text-heading">
                  {feature.title}
                </h3>
                <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                  {feature.description}
                </p>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* How it works */}
      <section className="border-t border-white/[0.06]">
        <div className="mx-auto max-w-4xl px-6 py-24">
          <div className="text-center">
            <h2 className="text-2xl font-bold tracking-tight text-heading sm:text-3xl">
              How it works
            </h2>
            <p className="mx-auto mt-3 max-w-lg text-sm text-muted-foreground sm:text-base">
              Three simple steps from upload to retrieval.
            </p>
          </div>

          <motion.div
            variants={container}
            initial="hidden"
            whileInView="show"
            viewport={{ once: true, margin: "-100px" }}
            className="mt-16 grid gap-8 sm:grid-cols-3"
          >
            {steps.map((step, idx) => (
              <motion.div
                key={step.title}
                variants={item}
                className="relative text-center"
              >
                {/* Step number */}
                <div className="mx-auto mb-4 flex h-14 w-14 items-center justify-center rounded-2xl bg-primary/10 ring-1 ring-white/[0.06]">
                  <step.icon className="h-6 w-6 text-primary" />
                </div>
                <div className="mb-1 text-xs font-medium text-muted-foreground">
                  Step {idx + 1}
                </div>
                <h3 className="text-base font-semibold text-heading">
                  {step.title}
                </h3>
                <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
                  {step.description}
                </p>
              </motion.div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* CTA */}
      <section className="border-t border-white/[0.06]">
        <div className="mx-auto max-w-4xl px-6 py-24 text-center">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
          >
            <h2 className="text-2xl font-bold tracking-tight text-heading sm:text-3xl">
              Ready to get started?
            </h2>
            <p className="mx-auto mt-3 max-w-md text-sm text-muted-foreground sm:text-base">
              Create an account and start uploading in seconds. No credit card
              required.
            </p>
            <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
              <Button size="lg" className="rounded-lg px-8" asChild>
                <Link to="/login">
                  Create Free Account
                  <ArrowRight className="ml-2 h-4 w-4" />
                </Link>
              </Button>
            </div>
          </motion.div>
        </div>
      </section>

      {/* Footer */}
      <footer className="border-t border-white/[0.06] bg-white/[0.02]">
        <div className="mx-auto flex max-w-6xl items-center justify-between px-6 py-6">
          <div className="flex items-center gap-2">
            <div className="flex h-6 w-6 items-center justify-center rounded-md bg-primary">
              <HardDrive className="h-3 w-3 text-primary-foreground" />
            </div>
            <span className="text-sm font-semibold text-muted-foreground">
              Mossaic
            </span>
          </div>
          <p className="text-xs text-muted-foreground">
            Built with Cloudflare Workers
          </p>
        </div>
      </footer>
    </div>
  );
}
