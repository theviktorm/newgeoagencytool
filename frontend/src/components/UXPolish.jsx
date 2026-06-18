/**
 * MOMENTUS AI — UX POLISH COMPONENTS
 * Empty states, loading states, error handling, confidence badges, approval gates UI
 * ═══════════════════════════════════════════════════════════════════════════════════
 */

import React from 'react';

// ═══════════════════════════════════════════════════════════════
// EMPTY STATES
// ═══════════════════════════════════════════════════════════════

export const EmptyState = ({ icon, title, description, action }) => (
  <div className="flex flex-col items-center justify-center py-12 px-4">
    <div className="text-6xl mb-4">{icon}</div>
    <h3 className="text-xl font-semibold text-gray-900 mb-2">{title}</h3>
    <p className="text-gray-500 text-center mb-6 max-w-md">{description}</p>
    {action && (
      <button className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
        {action.label}
      </button>
    )}
  </div>
);

export const EmptyProjects = () => (
  <EmptyState
    icon="📋"
    title="No GEO Projects Yet"
    description="Create your first GEO project to start managing your local search optimization."
    action={{ label: "Create Project" }}
  />
);

export const EmptyEntities = () => (
  <EmptyState
    icon="🏢"
    title="No Entities Onboarded"
    description="Add your first business entity to begin the optimization workflow."
    action={{ label: "Add Entity" }}
  />
);

export const EmptyImportBatches = () => (
  <EmptyState
    icon="📥"
    title="No Import Batches"
    description="Upload your first Peec data batch to start the import process."
    action={{ label: "Import Data" }}
  />
);

export const EmptyAudits = () => (
  <EmptyState
    icon="🔍"
    title="No Audits Run"
    description="Run a technical GEO audit to identify optimization opportunities."
    action={{ label: "Run Audit" }}
  />
);

export const EmptyApprovals = () => (
  <EmptyState
    icon="✓"
    title="No Pending Approvals"
    description="All items have been reviewed and approved."
  />
);

// ═══════════════════════════════════════════════════════════════
// LOADING STATES
// ═══════════════════════════════════════════════════════════════

export const LoadingSpinner = ({ size = "md", text = "Loading..." }) => {
  const sizeClasses = {
    sm: "w-4 h-4",
    md: "w-8 h-8",
    lg: "w-12 h-12",
  };

  return (
    <div className="flex flex-col items-center justify-center py-8">
      <div className={`${sizeClasses[size]} border-4 border-gray-200 border-t-blue-600 rounded-full animate-spin`} />
      {text && <p className="mt-4 text-gray-600">{text}</p>}
    </div>
  );
};

export const SkeletonLoader = ({ count = 3, type = "card" }) => {
  if (type === "card") {
    return (
      <div className="space-y-4">
        {[...Array(count)].map((_, i) => (
          <div key={i} className="bg-gray-200 rounded-lg h-24 animate-pulse" />
        ))}
      </div>
    );
  }

  if (type === "table") {
    return (
      <div className="space-y-2">
        {[...Array(count)].map((_, i) => (
          <div key={i} className="grid grid-cols-4 gap-4">
            {[...Array(4)].map((_, j) => (
              <div key={j} className="bg-gray-200 rounded h-6 animate-pulse" />
            ))}
          </div>
        ))}
      </div>
    );
  }

  return <LoadingSpinner text="Loading..." />;
};

// ═══════════════════════════════════════════════════════════════
// ERROR STATES
// ═══════════════════════════════════════════════════════════════

export const ErrorState = ({ title, message, action, details }) => (
  <div className="bg-red-50 border border-red-200 rounded-lg p-6">
    <div className="flex items-start">
      <div className="text-2xl mr-4">⚠️</div>
      <div className="flex-1">
        <h3 className="font-semibold text-red-900 mb-1">{title}</h3>
        <p className="text-red-700 mb-4">{message}</p>
        {details && (
          <pre className="bg-red-100 rounded p-3 text-xs text-red-900 overflow-auto mb-4">
            {details}
          </pre>
        )}
        {action && (
          <button className="px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700">
            {action.label}
          </button>
        )}
      </div>
    </div>
  </div>
);

export const ValidationError = ({ field, message }) => (
  <div className="text-red-600 text-sm mt-1">
    <span className="font-semibold">{field}:</span> {message}
  </div>
);

// ═══════════════════════════════════════════════════════════════
// CONFIDENCE BADGES
// ═══════════════════════════════════════════════════════════════

export const ConfidenceBadge = ({ score, label }) => {
  let bgColor, textColor, icon;

  if (score >= 90) {
    bgColor = "bg-green-100";
    textColor = "text-green-800";
    icon = "✓";
  } else if (score >= 70) {
    bgColor = "bg-yellow-100";
    textColor = "text-yellow-800";
    icon = "!";
  } else if (score >= 50) {
    bgColor = "bg-orange-100";
    textColor = "text-orange-800";
    icon = "△";
  } else {
    bgColor = "bg-red-100";
    textColor = "text-red-800";
    icon = "✕";
  }

  return (
    <span className={`inline-flex items-center gap-2 px-3 py-1 rounded-full text-sm font-medium ${bgColor} ${textColor}`}>
      <span className="font-bold">{icon}</span>
      <span>{label || `${Math.round(score)}%`}</span>
    </span>
  );
};

// ═══════════════════════════════════════════════════════════════
// APPROVAL GATE UI
// ═══════════════════════════════════════════════════════════════

export const ApprovalGateCard = ({ gate, onApprove, onReject, onRequestChanges }) => {
  const statusColors = {
    pending: "bg-yellow-50 border-yellow-200",
    approved: "bg-green-50 border-green-200",
    rejected: "bg-red-50 border-red-200",
    changes_requested: "bg-orange-50 border-orange-200",
  };

  const statusIcons = {
    pending: "⏳",
    approved: "✓",
    rejected: "✕",
    changes_requested: "↻",
  };

  return (
    <div className={`border rounded-lg p-6 ${statusColors[gate.status] || statusColors.pending}`}>
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-3">
          <span className="text-2xl">{statusIcons[gate.status]}</span>
          <div>
            <h4 className="font-semibold text-gray-900">{gate.gate_type}</h4>
            <p className="text-sm text-gray-600">Created {new Date(gate.created_at).toLocaleDateString()}</p>
          </div>
        </div>
        <span className="px-3 py-1 bg-white rounded text-sm font-medium capitalize">
          {gate.status.replace("_", " ")}
        </span>
      </div>

      {gate.status === "pending" && (
        <div className="flex gap-3">
          <button
            onClick={() => onApprove(gate.id)}
            className="flex-1 px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700"
          >
            Approve
          </button>
          <button
            onClick={() => onRequestChanges(gate.id)}
            className="flex-1 px-4 py-2 bg-orange-600 text-white rounded hover:bg-orange-700"
          >
            Request Changes
          </button>
          <button
            onClick={() => onReject(gate.id)}
            className="flex-1 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
          >
            Reject
          </button>
        </div>
      )}

      {gate.status === "approved" && gate.approval_notes && (
        <p className="text-sm text-gray-700 bg-white bg-opacity-50 rounded p-3">
          <strong>Approval Notes:</strong> {gate.approval_notes}
        </p>
      )}

      {gate.status === "rejected" && gate.rejection_reason && (
        <p className="text-sm text-red-700 bg-white bg-opacity-50 rounded p-3">
          <strong>Rejection Reason:</strong> {gate.rejection_reason}
        </p>
      )}

      {gate.status === "changes_requested" && gate.changes_needed && (
        <p className="text-sm text-orange-700 bg-white bg-opacity-50 rounded p-3">
          <strong>Changes Needed:</strong> {gate.changes_needed}
        </p>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// DATA QUALITY INDICATOR
// ═══════════════════════════════════════════════════════════════

export const DataQualityIndicator = ({ quality }) => {
  const completeness = ((quality.fields_present || 0) / (quality.total_fields || 1)) * 100;

  return (
    <div className="bg-white rounded-lg p-4 border border-gray-200">
      <h4 className="font-semibold text-gray-900 mb-3">Data Quality</h4>
      
      <div className="mb-4">
        <div className="flex justify-between items-center mb-2">
          <span className="text-sm text-gray-600">Completeness</span>
          <span className="text-sm font-semibold text-gray-900">{Math.round(completeness)}%</span>
        </div>
        <div className="w-full bg-gray-200 rounded-full h-2">
          <div
            className="bg-blue-600 h-2 rounded-full transition-all"
            style={{ width: `${completeness}%` }}
          />
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4 text-sm">
        <div>
          <p className="text-gray-600">Estimated Fields</p>
          <p className="text-lg font-semibold text-orange-600">{quality.estimated_field_count || 0}</p>
        </div>
        <div>
          <p className="text-gray-600">Missing Fields</p>
          <p className="text-lg font-semibold text-red-600">{quality.missing_field_count || 0}</p>
        </div>
      </div>

      {quality.average_confidence !== undefined && (
        <div className="mt-4 pt-4 border-t border-gray-200">
          <p className="text-sm text-gray-600 mb-2">Average Confidence</p>
          <ConfidenceBadge score={quality.average_confidence} />
        </div>
      )}
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// SUCCESS MESSAGE
// ═══════════════════════════════════════════════════════════════

export const SuccessMessage = ({ message, onDismiss }) => (
  <div className="bg-green-50 border border-green-200 rounded-lg p-4 flex items-center justify-between">
    <div className="flex items-center gap-3">
      <span className="text-2xl">✓</span>
      <p className="text-green-800">{message}</p>
    </div>
    {onDismiss && (
      <button
        onClick={onDismiss}
        className="text-green-600 hover:text-green-700 text-xl"
      >
        ✕
      </button>
    )}
  </div>
);

// ═══════════════════════════════════════════════════════════════
// PROGRESS INDICATOR
// ═══════════════════════════════════════════════════════════════

export const ProgressIndicator = ({ current, total, label }) => {
  const percentage = (current / total) * 100;

  return (
    <div className="bg-white rounded-lg p-4 border border-gray-200">
      <div className="flex justify-between items-center mb-2">
        <span className="text-sm font-semibold text-gray-900">{label}</span>
        <span className="text-sm text-gray-600">{current} of {total}</span>
      </div>
      <div className="w-full bg-gray-200 rounded-full h-3">
        <div
          className="bg-blue-600 h-3 rounded-full transition-all"
          style={{ width: `${percentage}%` }}
        />
      </div>
    </div>
  );
};

// ═══════════════════════════════════════════════════════════════
// ESTIMATED FIELD BADGE
// ═══════════════════════════════════════════════════════════════

export const EstimatedFieldBadge = ({ reason }) => (
  <span
    title={reason}
    className="inline-block px-2 py-1 bg-orange-100 text-orange-800 rounded text-xs font-medium cursor-help"
  >
    Estimated
  </span>
);

export const MissingFieldBadge = ({ impact }) => {
  const colors = {
    low: "bg-blue-100 text-blue-800",
    medium: "bg-yellow-100 text-yellow-800",
    high: "bg-red-100 text-red-800",
  };

  return (
    <span className={`inline-block px-2 py-1 rounded text-xs font-medium ${colors[impact] || colors.medium}`}>
      Missing ({impact})
    </span>
  );
};

// ═══════════════════════════════════════════════════════════════
// WORKFLOW STATUS BADGE
// ═══════════════════════════════════════════════════════════════

export const WorkflowStatusBadge = ({ status }) => {
  const colors = {
    pending: "bg-yellow-100 text-yellow-800",
    in_progress: "bg-blue-100 text-blue-800",
    completed: "bg-green-100 text-green-800",
    failed: "bg-red-100 text-red-800",
  };

  return (
    <span className={`inline-block px-3 py-1 rounded-full text-xs font-semibold ${colors[status] || colors.pending}`}>
      {status.replace("_", " ").toUpperCase()}
    </span>
  );
};
