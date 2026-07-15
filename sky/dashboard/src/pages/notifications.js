import React from 'react';
import Head from 'next/head';
import dynamic from 'next/dynamic';

const OperatorNotifications = dynamic(
  () =>
    import('@/components/operator-notifications').then(
      (mod) => mod.OperatorNotifications
    ),
  { ssr: false }
);

export default function OperatorNotificationsPage() {
  return (
    <>
      <Head>
        <title>Operator Notifications | SkyPilot Dashboard</title>
      </Head>
      <OperatorNotifications />
    </>
  );
}
