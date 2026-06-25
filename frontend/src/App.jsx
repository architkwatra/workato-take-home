import "./styles.css";

/**
 * Renders the placeholder dashboard shell until live pipeline views are added.
 */
export default function App() {
  return (
    <main className="shell">
      <section className="header">
        <p className="eyebrow">Workato Take-Home</p>
        <h1>Order Pipeline Dashboard</h1>
      </section>
      <section className="panel">
        <h2>Scaffold</h2>
        <p>Dashboard container is running. Live pipeline views will be added later.</p>
      </section>
    </main>
  );
}
