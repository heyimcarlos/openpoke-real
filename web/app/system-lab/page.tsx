import '@xyflow/react/dist/style.css';
import '@/components/system-lab/system-canvas.css';
import type { Metadata } from 'next';
import { OpenPokeSystemLab } from '@/components/system-lab/OpenPokeSystemLab';

export const metadata: Metadata = {
  title: 'OpenPoke Systems Lab',
  description: 'Interactive durable execution and scale-path architecture lab',
};

export default function SystemLabPage() {
  return <OpenPokeSystemLab />;
}
