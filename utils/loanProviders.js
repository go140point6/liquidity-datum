const LOAN_PROVIDER_CHOICES = [
  { name: "Enosys", value: "enosys" },
  { name: "PrimeFi", value: "primefi" },
];

function addLoanProviderOption(builder, { includeAll = false } = {}) {
  const choices = includeAll
    ? [...LOAN_PROVIDER_CHOICES, { name: "All", value: "all" }]
    : LOAN_PROVIDER_CHOICES;
  return builder.addStringOption((opt) =>
    opt
      .setName("provider")
      .setDescription("Loan provider")
      .setRequired(true)
      .addChoices(...choices)
  );
}

module.exports = {
  LOAN_PROVIDER_CHOICES,
  addLoanProviderOption,
};
